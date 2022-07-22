with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'customer') }}

),

final as (
    select
        customer_id::varchar,
        store_id::varchar,
        first_name::varchar,
        last_name::varchar,
        email::varchar,
        address_id::varchar,
        activebool::boolean,
        create_date::timestamp,
        last_update::timestamp,
        active::boolean
    from
        raw_data
)

select
    *
from
    final

