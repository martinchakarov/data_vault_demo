{%- macro generate_stage_model(entity) -%}

with raw_data as (
    select
        *
    from
    {% raw %}   {{ source('dvdrental',{% endraw %} '{{entity}}') }}

),

{%- set columns_query -%}
    select
        column_name
    from
        information_schema.columns
    where 
        table_schema = 'raw_data'
        and table_name   = '{{entity}}'
{%- endset -%}

{%- set results = run_query(columns_query) -%}


{%- if execute -%}

    {%- set results_list = results.columns[0].values() -%}

{%- else -%}}

    {%- set results_list = [] -%}}

{%- endif %}

final as (
    select
        {% for column in results_list -%}
        {{column}}
            {%- if not loop.last -%},{%- endif %}
        {% endfor -%}
    from
        raw_data
)

select
    *
from
    final

{% endmacro %}