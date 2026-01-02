import os
import datetime
import pandas as pd

from src.api import MaraviAPI
from src.calendar import TarponCalendar
from src.logger import setup_logger
from src.db import append_to_db, get_data_from_db, table_exists

logger = setup_logger(name="PL Fundos")

tarpon_calendar = TarponCalendar()


def append_entity_data(df, entity_type, id_column, schema="tarpon_base"):
    """
    Função genérica para adicionar dados de entidades ao banco de dados.

    Parâmetros:
    ----------
    df : DataFrame
        DataFrame contendo os dados a serem inseridos
    entity_type : str
        Nome da tabela no banco de dados (ex: "portfolio", "investor")
    id_column : str
        Nome da coluna que contém o ID da entidade (ex: "portfolio_id", "investor_id")
    name_column : str
        Nome da coluna que contém o nome da entidade (ex: "portfolio_name", "investor_name")
    schema : str, opcional
        Nome do schema no banco de dados (padrão: "teste123")
    """
    table_name = entity_type

    # Verifica se a tabela existe
    if not table_exists(table_name, schema):
        append_to_db(df, table_name=table_name, schema=schema)
    else:
        # Busca dados existentes no banco
        df_db = get_data_from_db(table_name, schema=schema)

        # Identifica novos registros para inserir
        df_to_insert = df[~df[id_column].isin(df_db[id_column].tolist())]

        # Insere novos registros se existirem
        if len(df_to_insert) > 0:
            logger.info(f"Inserindo novos {entity_type}s no banco de dados...\n")
            append_to_db(df_to_insert, table_name=table_name, schema=schema)
        else:
            logger.info(f"Nenhum novo {entity_type} para inserir\n")

def batch():
    datas = tarpon_calendar.get_business_days_in_range(datetime.date(2025, 7, 25), datetime.date(2025, 7, 25)) #yyyy,mm,dd
    
    for data in datas:
        #print(data)
        run(data)

def run(data=None):
    logger.info("Executando o script de preços...")

    if data is None:
        data = tarpon_calendar.get_previous_trading_day(datetime.date.today())

    logger.info("Buscando dados para: %s", data)

    MARAVI_USER = os.getenv("MARAVI_USER")
    MARAVI_PASS = os.getenv("MARAVI_PASS")
    MARAVI_CLIENT_ID = os.getenv("MARAVI_CLIENT_ID")
    MARAVI_CLIENT_SECRET = os.getenv("MARAVI_CLIENT_SECRET")

    logger.info("Conectando na API...")
    m = MaraviAPI(MARAVI_USER, MARAVI_PASS, MARAVI_CLIENT_ID, MARAVI_CLIENT_SECRET)
    m.authenticate()
    logger.info("Autenticado com sucesso!")
    
    payload = {
        "instrument_types": [3],
        "start_date": data.strftime("%Y-%m-%d"),
        "end_date": data.strftime("%Y-%m-%d"),
    }
    logger.info("Buscando dados na API...")
    df = m.fetch_data("market_data/pricing/prices/get", payload)
    logger.info("Dados obtidos com sucesso!")
    
    # Check if the DataFrame is empty or doesn't have the required columns
    if df.empty:
        logger.info(f"Nenhum dado de preço encontrado para a data: {data}")
        return
    
    required_columns = [
        "instrument","id", "instrument_id", "date",
        "fund_pl",
        "source_id"
    ]
    
    # Check if all required columns exist in the DataFrame
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"Colunas ausentes no DataFrame: {missing_columns}")
        logger.warning("Colunas disponíveis: " + ", ".join(df.columns.tolist()))
        return
        
    print("\n\n")
    df = df[
        [
            "instrument",
            "id",
            "instrument_id",
            "date",
            "fund_pl",
            "source_id"
        ]
    ].copy()

    # Filtrar apenas os source_id 15 e 11
    df = df[df["source_id"].isin([15, 11,7,33])].copy()
    logger.info(f"Filtrando apenas registros com source_id 15, 11 e 7. Total de registros: {len(df)}")

    numeric_columns = ["fund_pl"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert date columns
    date_columns = ["date"]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["instrument_id"] = df["instrument_id"].astype("Int64")

    # Se não há dados após o filtro, não fazer nada
    if df.empty:
        logger.info(f"Nenhum dado com source_id 15 ou 11 encontrado para a data: {data}")
        return

    # Para precos:
    df_precos = df[df["id"].notnull()].copy()
    append_entity_data(df_precos, "fund_pls", "id")
    print("\n")