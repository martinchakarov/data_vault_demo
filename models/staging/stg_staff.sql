with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'staff') }}

),

final as (
    select
        staff_id::varchar,
        first_name::varchar,
        last_name::varchar,
        address_id::varchar,
        email::varchar,
        store_id::varchar,
        active::boolean,
        username::varchar,
        password::varchar,
        last_update::timestamp,
        picture::bytea
    from
        raw_data
)

select
    *
from
    final

