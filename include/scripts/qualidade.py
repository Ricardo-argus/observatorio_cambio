import pandas as pd
import sqlalchemy as sa

CONN_STR = "postgresql+psycopg2://ricardo:Ric2026@postgres:5432/airflowproj"
engine = sa.create_engine(CONN_STR)


def validate_data():
    df = pd.read_sql("SELECT * FROM silver_cambio", engine)
    if (df["media_movel_venda"] <= 0).any():
        raise ValueError("Erro: Média inválida detectada!")
    print("Dados validados com sucesso")