with raw_data as (
    select
        *
    from
        {{ ref('stg_rental') }}
),

final as (
    select
        md5(rental_id || '_' || customer_id || '_' || inventory_id || '_' || staff_id) as hk_rental_id_customer_id_inventory_id_staff_id,
        md5(rental_id) as hk_rental_id,
        md5(customer_id) as hk_customer_id,
        md5(inventory_id) as hk_inventory_id,
        md5(staff_id) as hk_staff_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final