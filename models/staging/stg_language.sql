with raw_data as (
    select
        *
    from
       {{ source('dvdrental', 'language') }}

),

final as (
    select
        language_id::varchar,
        name::varchar,
        last_update::timestamp
    from
        raw_data
)

select
    *
from
    final

