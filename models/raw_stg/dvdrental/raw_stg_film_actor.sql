
{{ config(
    schema='raw_stg',
    materialized='view'
    ) }}

with raw_data as (
    select
        {{ dbt_utils.star(source('dvdrental', 'film_actor')) }}
    from
        {{ source('dvdrental', 'film_actor') }}
),

final as (
    select
        *
    from
        raw_data
)

select * from final

