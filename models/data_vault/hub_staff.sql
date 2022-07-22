with raw_data as (
    select
        *
    from
        {{ ref('stg_staff') }}
),

final as (
    select distinct
        md5(staff_id) as hk_staff_id,
        staff_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final