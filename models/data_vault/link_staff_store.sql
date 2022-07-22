with raw_data as (
    select
        staff_id,
        store_id
    from
        {{ ref('stg_staff') }}
),

final as (
    select distinct
        md5(staff_id || '_' || store_id) as hk_staff_id_store_id,
        md5(staff_id) as hk_staff_id,
        md5(store_id) as hk_store_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final