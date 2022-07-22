with raw_data as (
    select
        *
    from
        {{ ref('stg_film_actor') }}
),

final as (
    select
        md5(film_id || '_' || actor_id) as hk_film_id_actor_id,
        md5(film_id) as hk_film_id,
        md5(actor_id) as hk_actor_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final