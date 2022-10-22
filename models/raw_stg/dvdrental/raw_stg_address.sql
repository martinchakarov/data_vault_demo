
{{ config(
    schema='raw_stg',
    materialized='view'
    ) }}

with raw_data as (
    select
        {{ dbt_utils.star(source('dvdrental', 'address')) }}
    from
        {{ source('dvdrental', 'address') }}
),

final as (
    select
        *
    from
        raw_data
)

select * from final
