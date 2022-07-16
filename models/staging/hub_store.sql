with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'store') }}  
),

final as (
    select
        md5(store_id::varchar) as hk_store_id,
        store_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final