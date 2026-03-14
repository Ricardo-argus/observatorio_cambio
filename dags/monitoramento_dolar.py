from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from include.scripts.ingestao_api import ingest_data
from include.scripts.processamento import process_data

with DAG(
    dag_id="monitoramento_dolar",
    start_date=datetime(2024,1,1),
    schedule_interval = "@daily",
    catchup=False
) as dag:
    
    t1 = PythonOperator(task_id="ingestao", python_callable = ingest_data)
    t2 = PythonOperator(task_id="processamento", python_callable=process_data)

    t3 = BashOperator(
        task_id = "dbt_run",
        bash_command="cd /opt/airflow/include/analysys_dbt && dbt run --profiles-dir /opt/airflow/include/analysys_dbt"
    )

    t4 = BashOperator(
        task_id = "dbt_test",
        bash_command="cd /opt/airflow/include/analysys_dbt && dbt test --profiles-dir /opt/airflow/include/analysys_dbt"
    )



    t1 >> t2 >> t3 >> t4 
