with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'category') }}

),

final as (
    select
        category_id::varchar,
        name::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

