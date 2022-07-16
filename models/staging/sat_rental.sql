with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'rental') }}
),

final as (
    select
        md5(rental_id::varchar) as hk_rental_id,
        rental_date,
        return_date,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final