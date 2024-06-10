import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.cg_pipeline import cg_pipeline



default_args = {
    'owner': 'Jaryl Ngan',
    'start_date': datetime(2024, 6, 5)
}

dag = DAG(
    dag_id='etl_cg_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True,
    tags=['cg', 'etl', 'pipeline']
)

coins = ['bitcoin','ethereum']

for coin in coins:
    extract = PythonOperator(
        task_id= f'cg_extraction_{coin}',
        python_callable=cg_pipeline,
        op_kwargs={
            'coin_id': coin
        },
        dag=dag
    )


    upload_s3 = PythonOperator(
        task_id= f's3_upload_{coin}',
        python_callable=upload_s3_pipeline,
        op_kwargs={
                'coin_id': coin
            },
        dag=dag
    )

    extract >> upload_s3


