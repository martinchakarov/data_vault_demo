with raw_data as (
    select
        film_id,
        language_id
    from
        {{ source('dvdrental', 'film') }}
),

final as (
    select
        md5(film_id::varchar || '_' || language_id::varchar) as hk_film_id_language_id,
        md5(film_id::varchar) as hk_film_id,
        md5(language_id::varchar) as hk_language_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final