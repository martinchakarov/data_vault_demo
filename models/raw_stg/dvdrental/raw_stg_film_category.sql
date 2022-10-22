
    {{ config(
        schema='raw_stg',
        materialized='view'
        ) }}

    with raw_data as (
        select
            {{ dbt_utils.star(source('dvdrental', 'film_category')) }}
        from
            {{ source('dvdrental', 'film_category') }}
    ),

    final as (
        select
            *
        from
            raw_data
    )

    select * from final

    