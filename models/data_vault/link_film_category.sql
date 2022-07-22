with raw_data as (
    select
        *
    from
        {{ ref('stg_film_category') }}
),

final as (
    select
        md5(film_id || '_' || category_id) as hk_film_id_category_id,
        md5(film_id) as hk_film_id,
        md5(category_id) as hk_category_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final