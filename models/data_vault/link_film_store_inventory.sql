with raw_data as (
    select
        *
    from
        {{ ref('stg_inventory') }}
),

final as (
    select
        md5(film_id || '_' || store_id || '_' || inventory_id) as hk_film_id_store_id_inventory_id,
        md5(film_id) as hk_film_id,
        md5(store_id) as hk_store_id,
        md5(inventory_id) as hk_inventory_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final