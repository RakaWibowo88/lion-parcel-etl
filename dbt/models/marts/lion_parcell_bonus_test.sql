{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    tags = ['lion_parcell_bonus_test'],
) }}

select
    id,
    runtime_date,
    load_time,
    "Message"
from public.lion_parcell_bonus_test_stg