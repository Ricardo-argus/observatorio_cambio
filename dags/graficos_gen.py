from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from include.scripts.graficos import graph_py, graph_sql



with DAG(
    dag_id = 'graficos_dag',
    start_date=datetime(2024,1,1),
    schedule_interval = "@daily",
    catchup=False
) as dag:
    
    task_python = PythonOperator(task_id = "graficos_py", python_callable = graph_py)

    task_sql = PythonOperator(task_id = "graficos_sql", python_callable = graph_sql)

    task_python >> task_sql

