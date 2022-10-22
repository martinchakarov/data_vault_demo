from utils import get_sources, profile_sources
from paths import raw_stg_dir, create_dir_if_missing

import os
import subprocess
result = subprocess.run(['dbt', 'ls'], stdout=subprocess.PIPE)


try:
    print('Analyzing project structure...')

    cli_output = str(result.stdout).split('\\n')
    sources_split = get_sources(cli_output)

    # Create raw_stg directory if it does not exist
    create_dir_if_missing(raw_stg_dir)

    # Get an array of all sources defined in the dbt project
    sources = profile_sources(sources_split)

    for project, sources in sources.items():
        for source in sources:
            for source_name, tables in source.items():
                source_dir = os.path.join(raw_stg_dir, source_name)

                create_dir_if_missing(source_dir)

                print(f'\n===== Generating raw staging models for source {source_name} =====\n')
                # Create .sql files selecting everything from the sources
                for table in tables:
                    filepath = os.path.join(source_dir, f'raw_stg_{table}.sql')
                    with open(filepath, 'w') as f:
                    
                        file_content = f'''
{{{{ config(
    schema='raw_stg',
    materialized='view'
    ) }}}}

with raw_data as (
    select
        {{{{ dbt_utils.star(source('{source_name}', '{table}')) }}}}
    from
        {{{{ source('{source_name}', '{table}') }}}}
),

final as (
    select
        *
    from
        raw_data
)

select * from final

'''

                        
                        f.write(file_content)
                        print(f'Model {filepath} successfully created.')

    print('\n===== Running dbt models =====\n')
    dbt_run = subprocess.run(['dbt', 'run', '--select', 'raw_stg.*'])

    print('\n===== Creating schema.yml file =====\n')
    # Create a schema yml file for raw stg models
    dbt_source_yaml = subprocess.check_output(["dbt run-operation generate_source --args 'schema_name: raw_stg'"], shell=True)
    source_yml_dir = f'{raw_stg_dir}/schema.yml'
    with open(source_yml_dir, 'w') as f:

        # Remove unnecessary CLI text
        output = dbt_source_yaml.decode()
        start_index = output.index('version: 2')
        output = output[start_index:]
        f.write(output)

    print(f'Schema.yml file for raw staging models written to {source_yml_dir}')
except Exception:
    print('Something went wrong, please try again.')
finally:
    print('All done!')