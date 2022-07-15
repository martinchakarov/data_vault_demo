with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'actor') }}
),

final as (
    select  
        md5(actor_id::varchar) as hk_actor_id,
        current_timestamp as load_date,
        first_name,
        last_name,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final