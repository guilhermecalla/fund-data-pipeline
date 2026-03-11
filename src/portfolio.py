import datetime
import os
import pandas as pd
from src.calendar import TarponCalendar
from src.logger import setup_logger
from src.db import append_to_db, get_data_from_db, table_exists, engine
from src.api4 import MaraviAPI

logger = setup_logger(name="Carteiras")
tarpon_calendar = TarponCalendar()

def append_portfolio_data_simple(df, entity_type="fund_portfolio", schema="tarpon_base"):
    table_name = entity_type

    if df.empty:
        logger.info("DataFrame vazio, nada para inserir")
        return

    if not table_exists(table_name, schema):
        append_to_db(df, table_name=table_name, schema=schema)
        logger.info(f"Tabela {table_name} criada e {len(df)} registros inseridos")
        return

    logger.info(f"Verificando {len(df)} registros da API contra a base...")
    
    dates_to_check = df['date'].dt.strftime('%Y-%m-%d').unique()
    logger.info(f"Verificando dados para as datas: {dates_to_check}")
    
    date_filter = "', '".join(dates_to_check)
    query = f"""
    SELECT date, portfolio_name, instrument_name, asset_value, position_type, book_name
    FROM {schema}.{table_name}
    WHERE date::date IN ('{date_filter}')
    """
    
    try:
        df_existing = pd.read_sql(query, engine)
        logger.info(f"Encontrados {len(df_existing)} registros existentes na base")
    except Exception as e:
        logger.error(f"Erro ao buscar dados existentes: {e}")
        df_existing = pd.DataFrame()
    
    if not df_existing.empty:
        # Chave composta para dados agregados (incluindo book_name)
        df['composite_key'] = (
            df['portfolio_name'].astype(str) + '|' +
            df['date'].dt.strftime('%Y-%m-%d') + '|' +
            df['instrument_name'].astype(str) + '|' +
            df['position_type'].astype(str) + '|' +
            df['book_name'].astype(str)
        )

        df_existing['composite_key'] = (
            df_existing['portfolio_name'].astype(str) + '|' +
            df_existing['date'].astype(str) + '|' +
            df_existing['instrument_name'].astype(str) + '|' +
            df_existing['position_type'].astype(str) + '|' +
            df_existing['book_name'].astype(str)
        )
        
        existing_keys = set(df_existing['composite_key'].tolist())
        new_mask = ~df['composite_key'].isin(existing_keys)
        df_to_insert = df[new_mask].copy()
        df_to_insert = df_to_insert.drop('composite_key', axis=1)
        
        duplicated_count = len(df) - len(df_to_insert)
        logger.info(f"Registros duplicados (ignorados): {duplicated_count}")
        logger.info(f"Registros novos: {len(df_to_insert)}")
        
    else:
        df_to_insert = df.copy()

    if len(df_to_insert) > 0:
        try:
            logger.info(f"Inserindo {len(df_to_insert)} novos registros...")
            append_to_db(df_to_insert, table_name=table_name, schema=schema)
            logger.info("Inserção concluída com sucesso!")
        except Exception as e:
            logger.error(f"Erro ao inserir dados: {e}")
    else:
        logger.info("Nenhum registro novo para inserir")

def batch(start_date=None, end_date=None, stop_event=None):
    """Execução em lote para múltiplas datas"""
    if start_date is None:
        start_date = datetime.date(2025, 10, 31)
    if end_date is None:
        end_date = datetime.date(2025, 11, 28)

    for period in pd.period_range(start=start_date, end=end_date, freq='M'):
        if stop_event and stop_event.is_set():
            logger.info("Batch interrompido pelo usuário.")
            break
        date = period.to_timestamp()  # 1st of each month — avoids month-end edge cases
        data = tarpon_calendar.get_last_trading_day_of_month(date)
        run(data)

def run(data=None):
    logger.info("Executando o script de carteiras...")

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

    datef = data.strftime("%Y-%m-%d")
    params = {
        "start_date": datef,
        "end_date": datef,
        "instrument_position_aggregation": 3,
        "portfolio_ids": [
      1211,1924,1212,980,1216,616,1215,499,1213,657,775,732,964,774,984,427,161,505,1569,159,1158,
        1159,1576,824,879,164,145,653,1817,1816,1605,1606,1609,1610,1611,1680,1686,1687,1688,1692,1698,
        1699,1700,1731,1733,1734,1735,1704,1705,1706,1707,1708,1710,1617,1621,1622,1624,1626,1628,1779,
        1780,1788,1789,1790,1792,1793,1794,934,2345,1713,1722,1723,1724,1728,1726,1727,1797,1805,1806,1807,
        1811,1812,1813,1814,1815,1810,1539,1542,1543,1755,1756,1765,1766,1767,1772,1774,1775,1777,1776,1778,
        1769,144,740,2214
]
    }
    
    logger.info("Buscando dados na API...")
    df = m.fetch_data("portfolio_position/positions/get", params)
    logger.info("Dados obtidos com sucesso!")

    if df.empty:
        logger.info("Nenhum dado encontrado")
        return

    logger.info(f"Processando {len(df)} registros da API...")
    logger.info(f"Positions: {len(df[df['position_type'] == 'POSITION'])}")
    logger.info(f"Provisions: {len(df[df['position_type'] == 'PROVISION'])}")

    # ====== COLUNAS ESSENCIAIS COM AS NOVAS PERCENTUAIS ======
    desired_columns = [
        "date", "portfolio_name", "portfolio_id", "instrument_name", 
        "quantity", "price", "asset_value", "book_name", "position_type",
        "pct_net_asset_value", "pct_asset_value", "sector_name"
    ]
    
    # Filtrar apenas colunas que existem
    available_columns = [col for col in desired_columns if col in df.columns]
    logger.info(f"Usando {len(available_columns)} colunas essenciais")
    df = df[available_columns].copy()

    # Converter tipos ANTES da agregação
    numeric_columns = ["quantity", "price", "asset_value", "pct_net_asset_value", "pct_asset_value"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    if "portfolio_id" in df.columns:
        df["portfolio_id"] = pd.to_numeric(df["portfolio_id"], errors="coerce").astype("Int64")

    # Preencher valores nulos em sector_name para evitar perda de registros no groupby
    if "sector_name" in df.columns:
        df["sector_name"] = df["sector_name"].fillna("Não utilizar")

    # ====== AGREGAÇÃO: GROUP BY data, instrument_name, portfolio_name, position_type, book_name ======
    logger.info("Agregando dados por instrumento...")

    groupby_columns = ["date", "portfolio_name", "portfolio_id", "instrument_name", "position_type", "book_name", "sector_name"]
    
    # Agregações específicas para cada coluna
    agg_dict = {
        "asset_value": "sum",               # Somar asset_value
        "quantity": "sum",                  # Somar quantity
        "price": "mean",                    # Média do preço
        "pct_net_asset_value": "sum",       # Somar %Exposição
        "pct_asset_value": "sum"            # Somar %Vl. Financeiro
    }
    
    # Fazer a agregação
    df_aggregated = df.groupby(groupby_columns).agg(agg_dict).reset_index()
    
    logger.info(f"Dados agregados: {len(df)}  {len(df_aggregated)} registros")
    logger.info(f"Positions agregadas: {len(df_aggregated[df_aggregated['position_type'] == 'POSITION'])}")
    logger.info(f"Provisions agregadas: {len(df_aggregated[df_aggregated['position_type'] == 'PROVISION'])}")

    # Filtrar registros válidos
    df_valid = df_aggregated[df_aggregated["date"].notnull()].copy()
    
    logger.info(f"Registros válidos: {len(df_valid)}")
    append_portfolio_data_simple(df_valid)
    logger.info("Processo concluído!")