with raw_data as (
    select
        *
    from
        {{ ref('stg_payment') }}
),

final as (
    select
        md5(payment_id) as hk_payment_id,
        amount,
        payment_date,
        current_timestamp as load_date,
        'Postgres' as record_source
    from
        raw_data
)

select
    *
from
    final