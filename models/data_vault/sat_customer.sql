with raw_data as (
    select
        *
    from
        {{ ref('stg_customer') }}
),

final as (
    select distinct
        md5(raw_data.customer_id) as hk_customer_id,
        current_timestamp as load_date,
        first_name,
        last_name,
        email,
        create_date as created_on,
        active as is_active,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final