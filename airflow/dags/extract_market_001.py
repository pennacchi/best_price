from datetime import datetime, timedelta
from airflow import DAG
import sys
from pathlib import Path
from airflow.operators.python import PythonOperator

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))
from src.market_001.Extract import Extract
from src.market_001.Load import Load

default_args = {
    'owner': 'Penna',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 27),
    'retries': 0,
    'retry_delay': timedelta(seconds=20),
    'catchup': False
}


dag = DAG(
    'extract_market_001',
    schedule='0 10 * * *', # 07:00 AM (Brazil at UTC - 3 hours)
    default_args=default_args
)

extract_market_001 = PythonOperator(
    task_id='extract_market_001_with_python',
    python_callable=Extract.run,
    dag=dag
)

load_market_001_to_database = PythonOperator(
    task_id='load_market_001_to_database',
    python_callable=Load.run,
    dag=dag
)

extract_market_001 >> load_market_001_to_database