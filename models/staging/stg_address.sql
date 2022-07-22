with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'address') }}

),

final as (
    select
        address_id::varchar,
        address::varchar,
        address2::varchar,
        district::varchar,
        city_id::varchar,
        postal_code::varchar,
        phone::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

