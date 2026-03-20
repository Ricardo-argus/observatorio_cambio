from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from include.scripts.ingestao_dol import ingest_data
from include.scripts.ingestao_euro import ingest_euro_data
from include.scripts.processamento_dol import process_dol_data
from include.scripts.processamento_euro import process_euro_data
from include.scripts.variacoes_cambio import joins_cambio
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="monitoramento_cambial",
    start_date=datetime(2024,1,1),
    schedule_interval = "@daily",
    catchup=False
) as dag:
    
    with TaskGroup("pipeline_dolar") as pipeline_dolar:
        t1 = PythonOperator(task_id="ingestao_dolar", python_callable = ingest_data)
        t3 = PythonOperator(task_id="processamento_dol", python_callable=process_dol_data)
        t1 >> t3


    with TaskGroup("pipeline_euro") as pipeline_euro:
        t2 = PythonOperator(task_id="ingestao_euro", python_callable = ingest_euro_data)
        t4 = PythonOperator(task_id="processamento_euro", python_callable=process_euro_data)
        t2 >> t4

    t5 = PythonOperator(task_id="join_cambial", python_callable=joins_cambio)

    t6 = BashOperator(
        task_id = "dbt_run",
        bash_command="cd /opt/airflow/include/analysys_dbt && dbt run --profiles-dir /opt/airflow/include/analysys_dbt"
    )

    t7 = BashOperator(
        task_id = "dbt_test",
        bash_command="cd /opt/airflow/include/analysys_dbt && dbt test --profiles-dir /opt/airflow/include/analysys_dbt"
    )




    [pipeline_dolar, pipeline_euro] >> t5 >> t6 >> t7
