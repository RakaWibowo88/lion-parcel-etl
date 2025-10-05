{{ config(
    tags=['retail_transactions_scd'],
    materialized='incremental',
    unique_key=['id', 'valid_from'],
    on_schema_change='sync_all_columns'
) }}

with current_snapshot as (
  -- Ambil snapshot terbaru dari staging
  select
    id::bigint as id,
    customer_id::varchar(100) as customer_id,
    last_status::varchar(50) as last_status,
    pos_origin::varchar(200) as pos_origin,
    pos_destination::varchar(200) as pos_destination,
    created_at::timestamp with time zone as created_at,
    updated_at::timestamp with time zone as updated_at,
    -- Soft delete: jika status DONE, mark sebagai deleted
    case 
      when last_status = 'DONE' then updated_at::timestamp with time zone
      else null 
    end as deleted_at,
    updated_at::timestamp with time zone as snapshot_time
  from public.stg_retail_transactions
),

{% if is_incremental() %}
previous_current as (
  -- Ambil versi current dari target table
  select
    id,
    customer_id,
    last_status,
    pos_origin,
    pos_destination,
    created_at,
    updated_at,
    deleted_at,
    valid_from,
    valid_to,
    is_current,
    dw_inserted_at,
    dw_updated_at
  from {{ this }}
  where is_current = true
),

changes_detected as (
  -- Detect perubahan: compare current snapshot vs previous version
  select
    s.id,
    s.customer_id,
    s.last_status,
    s.pos_origin,
    s.pos_destination,
    s.created_at,
    s.updated_at,
    s.deleted_at,
    s.snapshot_time,
    
    case
      when p.id is null then 'INSERT'  -- row baru
      when (
        -- Cek apakah ada field yang berubah
        s.last_status is distinct from p.last_status or
        s.pos_origin is distinct from p.pos_origin or
        s.pos_destination is distinct from p.pos_destination or
        s.deleted_at is distinct from p.deleted_at
      ) then 'UPDATE'  -- ada perubahan
      else 'NO_CHANGE'  -- tidak ada perubahan
    end as change_type,
    
    p.valid_from as old_valid_from,
    p.dw_inserted_at as old_dw_inserted_at,
    p.dw_updated_at as old_dw_updated_at
    
  from current_snapshot s
  left join previous_current p on p.id = s.id
),

-- Step 1: Expire old versions (close validity period)
expired_versions as (
  select
    p.id::bigint as id,
    p.customer_id::varchar(100) as customer_id,
    p.last_status::varchar(50) as last_status,
    p.pos_origin::varchar(200) as pos_origin,
    p.pos_destination::varchar(200) as pos_destination,
    p.created_at::timestamp with time zone as created_at,
    p.updated_at::timestamp with time zone as updated_at,
    p.deleted_at::timestamp with time zone as deleted_at,
    p.valid_from::timestamp with time zone as valid_from,
    c.snapshot_time::timestamp with time zone as valid_to,
    false as is_current,
    p.dw_inserted_at::timestamp with time zone as dw_inserted_at,
    current_timestamp as dw_updated_at
  from previous_current p
  inner join changes_detected c
    on c.id = p.id
    and c.change_type = 'UPDATE'
),

-- Step 2: Create new versions untuk records yang berubah atau baru
new_versions as (
  select
    id::bigint as id,
    customer_id::varchar(100) as customer_id,
    last_status::varchar(50) as last_status,
    pos_origin::varchar(200) as pos_origin,
    pos_destination::varchar(200) as pos_destination,
    created_at::timestamp with time zone as created_at,
    updated_at::timestamp with time zone as updated_at,
    deleted_at::timestamp with time zone as deleted_at,
    snapshot_time::timestamp with time zone as valid_from,
    '9999-12-31 23:59:59'::timestamp as valid_to,
    true as is_current,
    coalesce(old_dw_inserted_at, snapshot_time)::timestamp with time zone as dw_inserted_at,
    current_timestamp as dw_updated_at
  from changes_detected
  where change_type in ('INSERT', 'UPDATE')
)

select * from (
  -- 1. Keep records yang tidak berubah (still current)
  select
    id,
    customer_id,
    last_status,
    pos_origin,
    pos_destination,
    created_at,
    updated_at,
    deleted_at,
    valid_from,
    valid_to,
    is_current,
    dw_inserted_at,
    dw_updated_at
  from previous_current
  where id not in (
    select id from changes_detected where change_type = 'UPDATE'
  )
  
  union all
  
  -- 2. Keep all historical records (is_current = false)
  select
    id,
    customer_id,
    last_status,
    pos_origin,
    pos_destination,
    created_at,
    updated_at,
    deleted_at,
    valid_from,
    valid_to,
    is_current,
    dw_inserted_at,
    dw_updated_at
  from {{ this }}
  where is_current = false
  
  union all
  
  -- 3. Add expired versions (close validity untuk record yang berubah)
  select
    id,
    customer_id,
    last_status,
    pos_origin,
    pos_destination,
    created_at,
    updated_at,
    deleted_at,
    valid_from,
    valid_to,
    is_current,
    dw_inserted_at,
    dw_updated_at
  from expired_versions
  
  union all
  
  -- 4. Add new versions (insert atau update)
  select
    id,
    customer_id,
    last_status,
    pos_origin,
    pos_destination,
    created_at,
    updated_at,
    deleted_at,
    valid_from,
    valid_to,
    is_current,
    dw_inserted_at,
    dw_updated_at
  from new_versions
)

{% else %}
-- Initial load (full refresh pertama kali)
select
  id::bigint as id,
  customer_id::varchar(100) as customer_id,
  last_status::varchar(50) as last_status,
  pos_origin::varchar(200) as pos_origin,
  pos_destination::varchar(200) as pos_destination,
  created_at::timestamp with time zone as created_at,
  updated_at::timestamp with time zone as updated_at,
  deleted_at::timestamp with time zone as deleted_at,
  snapshot_time::timestamp with time zone as valid_from,
  '9999-12-31 23:59:59'::timestamp as valid_to,
  true as is_current,
  snapshot_time::timestamp with time zone as dw_inserted_at,
  current_timestamp as dw_updated_at
from current_snapshot
{% endif %}

/*
═══════════════════════════════════════════════════════════════════════════════
SAMPLE OUTPUT FLOW:

Run 1 (2025-01-05 10:00:00) - Initial Insert:
┌────┬─────────────┬─────────┬────────────┬─────────────────────┬─────────────────────┬────────────┐
│ id │ last_status │ deleted │ valid_from          │ valid_to            │ is_current │
├────┼─────────────┼─────────┼─────────────────────┼─────────────────────┼────────────┤
│ 1  │ Created     │ NULL    │ 2025-01-05 10:00:00 │ 9999-12-31 23:59:59 │ TRUE       │
└────┴─────────────┴─────────┴─────────────────────┴─────────────────────┴────────────┘

Run 2 (2025-01-06 12:00:00) - Status Update (Created → On Way):
┌────┬─────────────┬─────────┬─────────────────────┬─────────────────────┬────────────┐
│ id │ last_status │ deleted │ valid_from          │ valid_to            │ is_current │
├────┼─────────────┼─────────┼─────────────────────┼─────────────────────┼────────────┤
│ 1  │ Created     │ NULL    │ 2025-01-05 10:00:00 │ 2025-01-06 12:00:00 │ FALSE      │ ← expired
│ 1  │ On Way      │ NULL    │ 2025-01-06 12:00:00 │ 9999-12-31 23:59:59 │ TRUE       │ ← new
└────┴─────────────┴─────────┴─────────────────────┴─────────────────────┴────────────┘

Run 3 (2025-01-08 14:00:00) - Status Update (On Way → Delivered):
┌────┬─────────────┬─────────┬─────────────────────┬─────────────────────┬────────────┐
│ id │ last_status │ deleted │ valid_from          │ valid_to            │ is_current │
├────┼─────────────┼─────────┼─────────────────────┼─────────────────────┼────────────┤
│ 1  │ Created     │ NULL    │ 2025-01-05 10:00:00 │ 2025-01-06 12:00:00 │ FALSE      │
│ 1  │ On Way      │ NULL    │ 2025-01-06 12:00:00 │ 2025-01-08 14:00:00 │ FALSE      │ ← expired
│ 1  │ Delivered   │ NULL    │ 2025-01-08 14:00:00 │ 9999-12-31 23:59:59 │ TRUE       │ ← new
└────┴─────────────┴─────────┴─────────────────────┴─────────────────────┴────────────┘

Run 4 (2025-01-10 16:00:00) - Soft Delete (Delivered → DONE):
┌────┬─────────────┬─────────────────────┬─────────────────────┬─────────────────────┬────────────┐
│ id │ last_status │ deleted_at          │ valid_from          │ valid_to            │ is_current │
├────┼─────────────┼─────────────────────┼─────────────────────┼─────────────────────┼────────────┤
│ 1  │ Created     │ NULL                │ 2025-01-05 10:00:00 │ 2025-01-06 12:00:00 │ FALSE      │
│ 1  │ On Way      │ NULL                │ 2025-01-06 12:00:00 │ 2025-01-08 14:00:00 │ FALSE      │
│ 1  │ Delivered   │ NULL                │ 2025-01-08 14:00:00 │ 2025-01-10 16:00:00 │ FALSE      │ ← expired
│ 1  │ DONE        │ 2025-01-10 16:00:00 │ 2025-01-10 16:00:00 │ 9999-12-31 23:59:59 │ TRUE       │ ← deleted!
└────┴─────────────┴─────────────────────┴─────────────────────┴─────────────────────┴────────────┘

═══════════════════════════════════════════════════════════════════════════════
USEFUL QUERIES:

-- 1. Current state only (like non-SCD table)
SELECT * FROM {{ this }} WHERE is_current = TRUE;

-- 2. Full history of specific transaction
SELECT 
  id, 
  last_status, 
  deleted_at,
  valid_from, 
  valid_to, 
  is_current
FROM {{ this }}
WHERE id = 1
ORDER BY valid_from;

-- 3. Time-travel query (state at specific date)
SELECT * FROM {{ this }}
WHERE '2025-01-07' BETWEEN valid_from AND valid_to;

-- 4. Audit trail: when did status change?
SELECT 
  id, 
  customer_id,
  last_status, 
  valid_from as changed_at,
  valid_to as valid_until
FROM {{ this }}
WHERE id = 1
ORDER BY valid_from;

-- 5. Count versions per transaction
SELECT 
  id, 
  COUNT(*) as version_count,
  MIN(valid_from) as first_seen,
  MAX(CASE WHEN is_current THEN valid_from END) as last_updated
FROM {{ this }}
GROUP BY id
ORDER BY version_count DESC;

-- 6. Deleted transactions (current state)
SELECT * FROM {{ this }}
WHERE is_current = TRUE
  AND deleted_at IS NOT NULL;

-- 7. Transactions deleted in last 7 days
SELECT * FROM {{ this }}
WHERE is_current = TRUE
  AND deleted_at >= CURRENT_DATE - INTERVAL '7 days';

-- 8. Active transactions only (not deleted)
SELECT * FROM {{ this }}
WHERE is_current = TRUE
  AND deleted_at IS NULL;
═══════════════════════════════════════════════════════════════════════════════
*/