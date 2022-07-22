with raw_data as (
    select
        *
    from
        {{ ref('stg_actor') }}  
),

final as (
    select
        md5(actor_id) as hk_actor_id,
        actor_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final