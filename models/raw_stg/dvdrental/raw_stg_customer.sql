
{{ config(
    schema='raw_stg',
    materialized='view'
    ) }}

with raw_data as (
    select
        {{ dbt_utils.star(source('dvdrental', 'customer')) }}
    from
        {{ source('dvdrental', 'customer') }}
),

final as (
    select
        *
    from
        raw_data
)

select * from final

