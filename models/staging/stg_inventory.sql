with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'inventory') }}

),

final as (
    select
        inventory_id::varchar,
        film_id::varchar,
        store_id::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

