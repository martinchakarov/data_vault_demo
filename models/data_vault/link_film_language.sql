with raw_data as (
    select
        film_id,
        language_id
    from
        {{ ref('stg_film') }}
),

final as (
    select
        md5(film_id || '_' || language_id) as hk_film_id_language_id,
        md5(film_id) as hk_film_id,
        md5(language_id) as hk_language_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final