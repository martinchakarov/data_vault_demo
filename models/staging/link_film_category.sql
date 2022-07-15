with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'film_category') }}
),

final as (
    select
        md5(film_id::varchar || '_' || category_id::varchar) as hk_film_id_category_id,
        md5(film_id::varchar) as hk_film_id,
        md5(category_id::varchar) as hk_category_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final