with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'category') }}
),

final as (
    select distinct
        md5(category_id::varchar) as hk_category_id,
        category_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final