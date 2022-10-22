
{{ config(
    schema='raw_stg',
    materialized='view'
    ) }}

with raw_data as (
    select
        {{ dbt_utils.star(source('dvdrental', 'city')) }}
    from
        {{ source('dvdrental', 'city') }}
),

final as (
    select
        *
    from
        raw_data
)

select * from final

