with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'film_category') }}

),

final as (
    select
        film_id::varchar,
        category_id::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

