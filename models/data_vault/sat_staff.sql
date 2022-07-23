with raw_data as (
    select
        *
    from
        {{ ref('stg_staff') }}
),

final as (
    select
        md5(staff_id) as hk_staff_id,
        current_timestamp as load_date,
        first_name,
        last_name,
        email,
        active as is_active,
        username,
        password,
        picture,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final