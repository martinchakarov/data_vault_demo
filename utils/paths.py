import os

# Main directories
project_dir = os.path.dirname(os.path.dirname(__file__))
models_dir = os.path.join(project_dir, 'models')
raw_stg_dir = os.path.join(models_dir, 'raw_stg')

def create_dir_if_missing(dir_name):
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
        print(f'Creating directory {dir_name}...')