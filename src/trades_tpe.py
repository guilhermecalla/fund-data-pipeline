import os
import datetime
import pandas as pd

from src.api3 import MaraviAPI
from src.calendar import TarponCalendar
from src.logger import setup_logger
from src.db import append_to_db, get_data_from_db, table_exists
import time


logger = setup_logger(name="Trades TPE")

tarpon_calendar = TarponCalendar()


def append_entity_data(df, entity_type, id_column, data, schema="tarpon_base"):
    """
    Função otimizada para adicionar dados de entidades ao banco de dados.
    Verifica apenas registros da data específica.
    """
    table_name = entity_type

    if df.empty:
        logger.info("DataFrame vazio, nada para inserir")
        return

    # Verifica se a tabela existe
    if not table_exists(table_name, schema):
        append_to_db(df, table_name=table_name, schema=schema)
        logger.info(f"Tabela {table_name} criada com {len(df)} registros")
        return

    logger.info(f"Verificando {len(df)} registros da API contra a base...")
    
    # Buscar apenas IDs da data específica (muito mais rápido)
    date_str = data.strftime('%Y-%m-%d')
    existing_ids_query = f"""
    SELECT DISTINCT {id_column} 
    FROM {schema}.{table_name} 
    WHERE date::date = '{date_str}' 
    AND {id_column} IS NOT NULL
    """
    
    try:
        from src.db import engine
        existing_ids_df = pd.read_sql(existing_ids_query, engine)
        existing_ids = existing_ids_df[id_column].astype(str).tolist() if not existing_ids_df.empty else []
        logger.info(f"Encontrados {len(existing_ids)} IDs já existentes na base para {date_str}")
    except Exception as e:
        logger.warning(f"Erro ao buscar IDs existentes: {e}. Assumindo tabela vazia.")
        existing_ids = []
    
    # Filtrar apenas registros novos
    df[id_column] = df[id_column].astype(str)
    df_new = df[~df[id_column].isin(existing_ids)].copy()
    
    
    duplicated_count = len(df) - len(df_new)
    logger.info(f"Total de registros: {len(df)}")
    logger.info(f"Registros que já existem (ignorados): {duplicated_count}")
    logger.info(f"Registros novos para inserir: {len(df_new)}")
    
    # Inserir apenas os registros novos
    if len(df_new) > 0:
        try:
            append_to_db(df_new, table_name=table_name, schema=schema)
            logger.info(f"Inseridos {len(df_new)} registros com sucesso")
        except Exception as e:
            logger.error(f"Erro ao inserir dados: {str(e)}")
            raise
    else:
        logger.info("Nenhum registro novo para inserir")


def batch():
    datas = tarpon_calendar.get_business_days_in_range(datetime.date(2020, 1, 1), datetime.date(2025, 8, 26))
    for data in datas:
        try:
            run(data)
            time.sleep(1)  # Pausa entre requisições
        except Exception as e:
            logger.error(f"Erro para data {data}: {e}")
            continue

def run(data=None):
    logger.info("Executando o script de operações...")

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
        "start_date": data.strftime("%Y-%m-%d"),
        "end_date": data.strftime("%Y-%m-%d"),
        "sides": [6, 7, 8, 4, 3, 1, 2, 5],
        #"operation_types": [1, 14],
        "instrument_group_ids": [8,11],
    }
    
    logger.info("Buscando dados na API...")
    df = m.fetch_data("operations/operations/get", payload)
    logger.info("Dados obtidos com sucesso!")

    if df.empty:
        logger.info(f"Nenhum dado de operação encontrado para a data: {data}")
        return

    desired_columns = [
        "id",
        "origin_id", 
        "portfolio_id",
        "portfolio_name", 
        "instrument_id", 
        "date", 
        "cash_settlement_date", 
        "quantity", 
        "instrument_symbol", 
        "side_name", 
        "unit_value", 
        "brokerage_fee_gross_value",
        "total_financial_net",
        "executing_brokerage_fee_value",
        "brokerage_fee_net_value",
        "carrying_brokerage_fee_value",
        "brokerage_rebate_value",
        "total_emoluments_value",
        "emoluments_value",
        "settlement_fee_value",
        "book_name", 
        "broker_name",
        "rebate_percent"
    ]
    
    available_columns = [col for col in desired_columns if col in df.columns]
    missing_columns = [col for col in desired_columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"Colunas ausentes no DataFrame: {missing_columns}")
        logger.info("Colunas disponíveis que serão utilizadas: " + ", ".join(available_columns))
    
    df = df[available_columns].copy()
    
    # Convert numeric columns
    numeric_columns = [
        "quantity", "unit_value", "brokerage_fee_gross_value", "total_financial_net",
        "executing_brokerage_fee_value", "brokerage_fee_net_value", "carrying_brokerage_fee_value",
        "brokerage_rebate_value", "total_emoluments_value", "emoluments_value", 
        "settlement_fee_value", "rebate_percent"
    ]
    
    existing_numeric_columns = [col for col in numeric_columns if col in df.columns]
    for col in existing_numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert date columns  
    date_columns = ["date", "cash_settlement_date"]
    existing_date_columns = [col for col in date_columns if col in df.columns]
    for col in existing_date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Convert ID columns to Int64
    id_columns = ["portfolio_id", "instrument_id", "origin_id"]
    existing_id_columns = [col for col in id_columns if col in df.columns]
    for col in existing_id_columns:
        df[col] = df[col].astype("Int64")

    # Para operações - passa a data como parâmetro
    df_operations = df[df["id"].notnull()].copy()
    append_entity_data(df_operations, "operations", "id", data)

