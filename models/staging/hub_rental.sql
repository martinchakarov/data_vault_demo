with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'rental') }}
),

final as (
    select distinct
        md5(rental_id::varchar) as hk_rental_id,
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