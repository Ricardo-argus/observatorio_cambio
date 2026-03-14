import os
import requests
import sqlalchemy as sa
from datetime import datetime

# Usa a mesma string de conexão definida no docker-compose
CONN_STR = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

engine = sa.create_engine(CONN_STR)

def ingest_data():

    url = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%2701-01-2025%27&@dataFinalCotacao=%2703-11-2026%27&$top=100&$format=json"
    )
    response = requests.get(url).json()

    for item in response["value"]:
        df = {
            "cotacao_compra": float(item["cotacaoCompra"]),
            "cotacao_venda": float(item["cotacaoVenda"]),
            "datahoracotacao": datetime.strptime(item["dataHoraCotacao"], "%Y-%m-%d %H:%M:%S.%f")
        }

        with engine.begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO public.bronze_cambio (
                    cotacao_compra, cotacao_venda, datahoracotacao
                ) VALUES (
                    :cotacao_compra, :cotacao_venda, :datahoracotacao
                )
            """), df)