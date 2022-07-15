with raw_data as (
    select
        *
    from
        {{ source('dvdrental', 'language') }}
),

final as (
    select
        md5(language_id::varchar) as hk_language_id,
        language_id,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final