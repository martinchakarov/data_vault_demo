with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'actor') }}

),

final as (
    select
        actor_id::varchar,
        first_name::varchar,
        last_name::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

