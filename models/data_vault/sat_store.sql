with raw_data as (
    select
        *
    from
        {{ ref('stg_store') }}
),

addresses as (
    select
        *
    from
        {{ ref('stg_address') }}
    where
        address_id in (select distinct address_id from raw_data)
),

cities as (
    select
        *
    from
        {{ ref('stg_city') }}
    where
        city_id in (select distinct city_id from addresses)
),

countries as (
    select
        *
    from
        {{ ref('stg_country') }}
    where
        country_id in (select distinct country_id from cities)
),

full_addresses as (
    select
        *
    from
        addresses
        left join cities
        on addresses.city_id = cities.city_id
        left join countries
        on cities.country_id = countries.country_id
),

final as (
    select
        md5(raw_data.store_id) as hk_store_id,
        current_timestamp as load_date,
        full_addresses.address,
        full_addresses.district,
        full_addresses.postal_code,
        full_addresses.phone,
        full_addresses.city,
        full_addresses.country,
        'Postgres' as record_source
    from
        raw_data
        left join full_addresses
        on raw_data.address_id = full_addresses.address_id
)

select
    *
from
    final