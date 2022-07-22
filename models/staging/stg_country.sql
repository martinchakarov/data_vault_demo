with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'country') }}

),

final as (
    select
        country_id::varchar,
        country::varchar,
        last_update::timestamp
        from
        raw_data
)

select
    *
from
    final

