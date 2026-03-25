# Customização da imagem do Airflow (para instalar dbt e pandas)
FROM apache/airflow:2.7.0

# Instala dbt para Postgres
RUN pip install dbt-postgres

# Bibliotecas Python para gráficos
RUN pip install seaborn matplotlib pandas numpy sqlalchemy psycopg2-binary
