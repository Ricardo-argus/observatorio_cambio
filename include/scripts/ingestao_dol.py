import os
import requests
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert


# Usa a mesma string de conexão definida no docker-compose
CONN_STR = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

engine = sa.create_engine(CONN_STR)

def ingest_data():

    url = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='USD'&@dataInicial='01-01-2025'&@dataFinalCotacao='01-01-2026'&$top=1000&$filter=tipoBoletim%20eq%20'Fechamento'&$orderby=dataHoraCotacao&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"
    )
    response = requests.get(url).json()

    rows = []
    for item in response["value"]:
        rows.append({
            "cotacao_compra": float(item["cotacaoCompra"]),
            "cotacao_venda": float(item["cotacaoVenda"]),
            "datahoracotacao": datetime.strptime(item["dataHoraCotacao"], "%Y-%m-%d %H:%M:%S.%f"),
            "tipoboletim": str(item["tipoBoletim"])
        })

    # Faz o upsert usando datahoracotacao como chave única
    with engine.begin() as conn:
        table = sa.Table("bronze_dol_cambio", sa.MetaData(), autoload_with=engine)
        stmt = insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["datahoracotacao"],  # precisa ser UNIQUE na tabela
            set_={
                "cotacao_compra": stmt.excluded.cotacao_compra,
                "cotacao_venda": stmt.excluded.cotacao_venda,
                "tipoboletim": stmt.excluded.tipoboletim
            }
        )
        conn.execute(stmt)
