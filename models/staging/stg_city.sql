with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'city') }}

),

final as (
    select
        city_id::varchar,
        city::varchar,
        country_id::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

