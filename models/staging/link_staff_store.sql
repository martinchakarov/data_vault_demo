with raw_data as (
    select
        staff_id,
        store_id
    from
        {{ source('dvdrental', 'staff') }}
),

final as (
    select distinct
        md5(staff_id::varchar || '_' || store_id::varchar) as hk_staff_id_store_id,
        md5(staff_id::varchar) as hk_staff_id,
        md5(store_id::varchar) as hk_store_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final