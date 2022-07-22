with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'rental') }}

),

final as (
    select
        rental_id::varchar,
        rental_date::timestamp,
        inventory_id::varchar,
        customer_id::varchar,
        return_date::timestamp,
        staff_id::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

