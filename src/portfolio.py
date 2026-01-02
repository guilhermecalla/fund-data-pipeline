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
    SELECT date, portfolio_name, instrument_name, asset_value, position_type
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
        # Chave composta para dados agregados
        df['composite_key'] = (
            df['portfolio_name'].astype(str) + '|' +
            df['date'].dt.strftime('%Y-%m-%d') + '|' +
            df['instrument_name'].astype(str) + '|' +
            df['position_type'].astype(str)
        )
        
        df_existing['composite_key'] = (
            df_existing['portfolio_name'].astype(str) + '|' +
            df_existing['date'].astype(str) + '|' +
            df_existing['instrument_name'].astype(str) + '|' +
            df_existing['position_type'].astype(str)
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

def batch():
    """Execução em lote para múltiplas datas"""
    datas = pd.date_range(datetime.date(2025, 10, 31), datetime.date(2025,11, 28))
    
    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()
    
    for date in df.date.values:
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
        "portfolio_ids": [875,1158,1159,1160,1576,1308,843,
            427,984,144,732,506,161,964,685,499,
            775,1298,934,1215,1299,1213,
            657,1211,980,616,1184,1137,1277,
            1212,1216,774,1303,159,1274,824,1569,
            653,950,879,164,505,145,1924,1987,1539]
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

    # ====== AGREGAÇÃO: GROUP BY data, instrument_name, portfolio_name, position_type ======
    logger.info("Agregando dados por instrumento...")
    
    groupby_columns = ["date", "portfolio_name", "portfolio_id", "instrument_name", "position_type","sector_name"]
    
    # Agregações específicas para cada coluna
    agg_dict = {
        "asset_value": "sum",               # Somar asset_value
        "quantity": "sum",                  # Somar quantity
        "price": "mean",                    # Média do preço
        "book_name": "first",               # Primeiro book_name
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