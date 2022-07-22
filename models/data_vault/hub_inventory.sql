with raw_data as (
    select
        *
    from
        {{ ref('stg_inventory') }}
),

final as (
    select
        md5(inventory_id) as hk_inventory_id,
        inventory_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final