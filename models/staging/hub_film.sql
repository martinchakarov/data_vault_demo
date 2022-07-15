with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'film') }}
),

final as (
    select distinct
        md5(film_id::varchar) as hk_film_id,
        film_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final