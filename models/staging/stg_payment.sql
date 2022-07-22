with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'payment') }}

),

final as (
    select
        payment_id::varchar,
        customer_id::varchar,
        staff_id::varchar,
        rental_id::varchar,
        amount::numeric(5, 2),
        payment_date::timestamp
    from
        raw_data
)

select
    *
from
    final

