# Onde ficam seus arquivos de definição do fluxo (DAGs)│   └── monitoramento_dolar.py     # O código que orquestra as Tasks 1, 2, 3 e 4

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.scripts.ingestao_api import ingest_data
from include.scripts.processamento import process_data
from include.scripts.qualidade import validate_data

with DAG(
    dag_id="monitoramento_dolar",
    start_date=datetime(2024,1,1),
    schedule_interval = "@daily",
    catchup=False
) as dag:
    
    t1 = PythonOperator(task_id="ingestao", python_callable = ingest_data)
    t2 = PythonOperator(task_id="processamento", python_callable=process_data)
    t3 = PythonOperator(task_id="qualidade", python_callable=validate_data)


    t1 >> t2 >> t3 
