with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'customer') }}
),

final as (
    select distinct
        md5(customer_id::varchar) as hk_customer_id,
        customer_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final