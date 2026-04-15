import subprocess, os
from prefect         import flow, task
from prefect.logging import get_run_logger
from datetime        import datetime


DBT_DIR = os.path.join(os.path.dirname(__file__), '..', 'dbt_chicago')


@task(name='dbt-run', retries=1)
def run_dbt():
    logger = get_run_logger()
    r = subprocess.run(['dbt','run','--profiles-dir',DBT_DIR],
                       cwd=DBT_DIR, capture_output=True, text=True)
    logger.info(r.stdout)
    if r.returncode != 0: raise RuntimeError(f'dbt run failed:\n{r.stderr}')


@task(name='dbt-test', retries=1)
def run_dbt_tests():
    logger = get_run_logger()
    r = subprocess.run(['dbt','test','--profiles-dir',DBT_DIR],
                       cwd=DBT_DIR, capture_output=True, text=True)
    logger.info(r.stdout)
    if r.returncode != 0: raise RuntimeError(f'dbt test failed:\n{r.stderr}')


@task(name='train-model')
def train_model():
    # load_dotenv must be called inside the task — 
    # Prefect tasks run in their own context and do not
    # inherit os.environ from module-level load
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(r"C:\Users\brand\portfolio\Tip-Classification\.env"), override=True)

    import pandas as pd, mlflow, mlflow.sklearn, snowflake.connector
    from sklearn.ensemble        import GradientBoostingClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics         import roc_auc_score
    from mlflow.models.signature import infer_signature

    logger = get_run_logger()

    cfg = {
        'user':      os.environ['SF_USER'],
        'password':  os.environ['SF_PASSWORD'],
        'account':   os.environ['SF_ACCOUNT'],
        'warehouse': 'TAXI_WH',
        'database':  'TAXI_DB',
        'schema':    'MARTS',
    }

@flow(name='chicago-taxi-ml-pipeline', log_prints=True)
def pipeline():
    run_dbt()
    run_dbt_tests()
    train_model()


if __name__ == '__main__':
    pipeline()
