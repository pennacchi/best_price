from datetime import datetime, timedelta
from airflow import DAG
import sys
from pathlib import Path
from airflow.operators.python import PythonOperator

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))
from src.market_001.Extract import Extract

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
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

extract_market_001_with_python = PythonOperator(
    task_id='extract_market_001_with_python',
    python_callable=Extract.run,
    dag=dag
)

extract_market_001_with_python