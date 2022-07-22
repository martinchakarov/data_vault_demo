with raw_data as (
    select
        customer_id,
        store_id
    from
        {{ ref('stg_customer') }}
),

final as (
    select distinct
        md5(customer_id || '_' || store_id) as hk_customer_id_store_id,
        md5(customer_id) as hk_customer_id,
        md5(store_id) as hk_store_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final