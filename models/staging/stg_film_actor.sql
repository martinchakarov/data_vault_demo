with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'film_actor') }}

),

final as (
    select
        actor_id::varchar,
        film_id::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

