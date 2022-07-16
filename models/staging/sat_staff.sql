with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'staff') }}
),

final as (
    select
        md5(staff_id::varchar) as hk_staff_id,
        first_name,
        last_name,
        email,
        active as is_active,
        username,
        password,
        picture,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final