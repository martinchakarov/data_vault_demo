with raw_data as (
    select
        *
    from
        {{ ref('stg_rental') }}
),

final as (
    select distinct
        md5(rental_id) as hk_rental_id,
        rental_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final