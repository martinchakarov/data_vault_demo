with raw_data as (
    select
        *
    from
        {{ ref('stg_category') }}
),

final as (
    select distinct
        md5(category_id) as hk_category_id,
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