import pandas as pd
import sqlalchemy as sa
import os

CONN_STR = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
engine = sa.create_engine(CONN_STR)


def validate_data():
    df = pd.read_sql("SELECT * FROM silver_cambio", engine)
    if (df["media_movel_venda"] <= 0).any():
        raise ValueError("Erro: Média inválida detectada!")
    print("Dados validados com sucesso")