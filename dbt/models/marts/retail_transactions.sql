{{ config(
    tags=['retail_transactions'],
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with src as (
  select
    id,
    customer_id,
    last_status,
    pos_origin,
    pos_destination,
    created_at,
    now() as _run_ts
  from public.stg_retail_transactions
),

final as (
  select
    s.id,
    s.customer_id,
    s.last_status,
    s.pos_origin,
    s.pos_destination,

    -- created_at: pakai yang dari sumber; kalau null (insert pertama) isi _run_ts
    coalesce(s.created_at, s._run_ts) as created_at,

    -- updated_at: selalu timestamp run saat baris ini ditulis (insert/update)
    s._run_ts as updated_at,

    -- deleted_at: isi saat status 'DONE'. Jika sudah pernah terisi, pertahankan nilainya.
    case
      when s.last_status = 'DONE' then
        {% if is_incremental() %}
          coalesce(t.deleted_at, s._run_ts)
        {% else %}
          s._run_ts
        {% endif %}
      else null
    end as deleted_at
  from src s
  {% if is_incremental() %}
  left join {{ this }} t on t.id = s.id
  {% endif %}
)

select * from final