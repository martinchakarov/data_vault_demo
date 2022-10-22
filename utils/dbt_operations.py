import os
import subprocess
from paths import raw_stg_dir

def run_dbt_models(folder):
    print('\n===== Running dbt models =====\n')
    dbt_run = subprocess.run(['dbt', 'run', '--select', f'{folder}.*'])

def generate_model_yml(dir):
    print('\n===== Creating .yml files for models =====\n')
    print('This might take a while...')
    for subdir in os.listdir(dir):
        
        full_subdir_path = os.path.join(dir, subdir)
        
        file_count = len([name for name in os.listdir(full_subdir_path) if os.path.isfile(os.path.join(full_subdir_path, name)) and name.endswith('.sql')])
        done_count = 1 

        for file in os.listdir(full_subdir_path):
            if file.endswith('.sql'):
                model = file[:-4]

                dbt_source_yaml = subprocess.check_output([f"dbt run-operation generate_model_yaml --args 'model_name: {model}'"], shell=True)

                # Remove unnecessary CLI text, if any
                output = dbt_source_yaml.decode()
                start_index = output.index('version: 2')
                output = output[start_index:]

                filepath = f'{full_subdir_path}/{model}.yml'

                with open(filepath, 'w') as f:
                    f.write(output)
                    print(f'[{done_count}/{file_count}] Successfully created property file for model {model} at {filepath}')
                    done_count = done_count + 1

