with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'rental') }}
),

final as (
    select
        md5(rental_id::varchar || '_' || customer_id::varchar || '_' || inventory_id::varchar || '_' || staff_id::varchar) as hk_rental_id_customer_id_inventory_id_staff_id,
        md5(rental_id::varchar) as hk_rental_id,
        md5(customer_id::varchar) as hk_customer_id,
        md5(inventory_id::varchar) as hk_inventory_id,
        md5(staff_id::varchar) as hk_staff_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final