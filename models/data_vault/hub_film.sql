with raw_data as (
    select
        *
    from
        {{ ref('stg_film') }}
),

final as (
    select distinct
        md5(film_id) as hk_film_id,
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