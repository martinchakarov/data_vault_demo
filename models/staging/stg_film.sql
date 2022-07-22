with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'film') }}

),

final as (
    select
        film_id::varchar,
        title::varchar,
        description::varchar,
        release_year::integer,
        language_id::varchar,
        rental_duration::integer,
        rental_rate::numeric(4,2),
        length::integer,
        replacement_cost::numeric(5, 2),
        rating::varchar,
        last_update::timestamp,
        special_features::text[],
        fulltext::tsvector
    from
        raw_data
)

select
    *
from
    final

