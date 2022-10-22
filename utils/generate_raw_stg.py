from utils import create_source_list
from paths import raw_stg_dir
from dbt_operations import generate_model_yml, run_dbt_models


try:
    create_source_list()

    run_dbt_models('raw_stg')

    generate_model_yml(raw_stg_dir)

except Exception as e:
    print('Something went wrong, please try again.')
    print(e)
finally:
    print('All done!')