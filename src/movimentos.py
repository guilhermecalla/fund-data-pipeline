import os
import datetime
import pandas as pd

from src.api import MaraviAPI
from src.calendar import TarponCalendar
from src.logger import setup_logger
from src.db import append_to_db, get_data_from_db, table_exists

logger = setup_logger(name="Movimentos")

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
        Nome do schema no banco de dados (padrão: "tarpon_base")
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
    #datas = tarpon_calendar.get_business_days_in_range(datetime.date(2006, 10, 1), datetime.date(2015, 12, 18)) #yyyy,mm,dd
    datas = tarpon_calendar.get_business_days_in_range(datetime.date(2025, 7, 31), datetime.date(2025, 9, 25)) #yyyy,mm,dd
    
    for data in datas:
        print(data.strftime("%Y-%m-%d"))
        run(data)

def run(data=None):
    logger.info("Executando o script de movimentação...")

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
        "include_administrator_account_group_ids_by_transaction": "true",
        "request_start_date": data.strftime("%Y-%m-%d"),
        "request_end_date": data.strftime("%Y-%m-%d"),
        "status": [3,2],
    }
    logger.info("Buscando dados na API...")
    df = m.fetch_data("liabilities/transaction_order/get", payload)
    logger.info("Dados obtidos com sucesso!")

    # Check if the DataFrame is empty or doesn't have the required columns
    if df.empty:
        logger.info(f"Nenhum dado encontrado para a data: {data}")
        return
    
    required_columns = [
        "id", "portfolio_id", "portfolio_name", "investor_id", "investor_name",
        "distributor_id", "distributor_name", "transaction_type_description",
        "net_financial_value", "request_date", "conversion_date", "payment_date",
        "investor_legal_id", "investor_legal_entity_type", "account_group_name",
        "investor_custody_account_name", "navps", "shares_amount", "invested_book_id"
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
            "id",
            "portfolio_id",
            "portfolio_name",
            "investor_id",
            "investor_name",
            "distributor_id",
            "distributor_name",
            "transaction_type_description",
            "net_financial_value",
            "request_date",
            "conversion_date",
            "payment_date",
            "investor_legal_id",
            "investor_legal_entity_type",
            "account_group_name",
            "investor_custody_account_name",
            "navps",
            "shares_amount",
            "invested_book_id",
        ]
    ].copy()

    numeric_columns = ["net_financial_value", "navps", "shares_amount"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert date columns
    date_columns = ["request_date", "conversion_date", "payment_date"]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["portfolio_id"] = df["portfolio_id"].astype("Int64")
    df["investor_id"] = df["investor_id"].astype("Int64")
    df["distributor_id"] = df["distributor_id"].astype("Int64")

    # Para portfolios:
    df_portfolio = df[["portfolio_id", "portfolio_name","invested_book_id"]].drop_duplicates()
    df_portfolio = df_portfolio[df_portfolio["portfolio_id"].notnull()].copy()
    append_entity_data(df_portfolio, "portfolio", "portfolio_id")
    print("\n")

    # Para investors:
    df_investor = df[["investor_id", "investor_name"]].drop_duplicates()
    df_investor = df_investor[df_investor["investor_id"].notnull()].copy()
    append_entity_data(df_investor, "investor", "investor_id")
    print("\n")

    # Para distribuidor:
    df_distributor = df[["distributor_id", "distributor_name"]].drop_duplicates()
    df_distributor = df_distributor[df_distributor["distributor_id"].notnull()].copy()
    append_entity_data(df_distributor, "distributor", "distributor_id")
    print("\n")

    # Para movimentações:
    df_movements = df[df["id"].notnull()].copy()
    append_entity_data(df_movements, "movements", "id")
    print("\n")