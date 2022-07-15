with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'film_actor') }}
),

final as (
    select
        md5(film_id::varchar || '_' || actor_id::varchar) as hk_film_id_actor_id,
        md5(film_id::varchar) as hk_film_id,
        md5(actor_id::varchar) as hk_actor_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final