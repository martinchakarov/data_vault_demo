with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'store') }}

),

final as (
    select
        store_id::varchar,
        manager_staff_id::varchar,
        address_id::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

