import subprocess
import os
from paths import create_dir_if_missing, raw_stg_dir

def get_sources(cli_output):
    sources = []

    for line in cli_output:
        if line.startswith('source:'):
            sources.append(line[7:])
    
    return sources

def schema_exists(schema_name, source_list):
    
    for source in source_list:
        if schema_name in source.keys():
            return True
        
    return False

def get_schema(sources, project, schema_name):
    
    for element in sources[project]:
        if schema_name in element:
            return element[schema_name]
    

def profile_sources(projects_arr):
    projects = {}
    for source in projects_arr:
        [project, schema, table] = source.split('.')

        if project not in projects:
            projects[project] = [{schema: [table]}]
        elif not schema_exists(schema, projects[project]):
            projects[project].append({schema: [table]})
        elif schema_exists(schema, projects[project]):
            tables = get_schema(projects, project, schema)
            if table not in tables:
                tables.append(table)
    return projects

def create_source_list():
        print('Analyzing project structure...')
        
        result = subprocess.run(['dbt', 'ls'], stdout=subprocess.PIPE)
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