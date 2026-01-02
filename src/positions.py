import os
import datetime
import pandas as pd
import numpy as np

from src.api2 import MaraviAPI
from src.calendar import TarponCalendar
from src.logger import setup_logger
from src.db import append_to_db, get_data_from_db, table_exists, engine

logger = setup_logger(name="Posições")

tarpon_calendar = TarponCalendar()


def append_positions_data_simple(df, entity_type="positions", schema="tarpon_base"):
    """
    Função simplificada para adicionar dados de posições ao banco de dados.
    Remove duplicatas internas e compara com base usando chave composta.
    """
    table_name = entity_type

    if df.empty:
        logger.info("DataFrame vazio, nada para inserir")
        return

    # Verifica se a tabela existe
    if not table_exists(table_name, schema):
        append_to_db(df, table_name=table_name, schema=schema)
        logger.info(f"Tabela {table_name} criada e {len(df)} registros inseridos")
        return

    logger.info(f"Verificando {len(df)} registros da API contra a base...")
    
    # Buscar apenas os dados da mesma data que estamos tentando inserir
    dates_to_check = df['date'].dt.strftime('%Y-%m-%d').unique()
    logger.info(f"Verificando dados para as datas: {dates_to_check}")
    
    # Criar query para buscar registros existentes da mesma data
    date_filter = "', '".join(dates_to_check)
    query = f"""
    SELECT portfolio_name, date, investor_names, distributor_name, account_group_names,
           shares_amount, financial_value, participation_in_portfolio
    FROM {schema}.{table_name} 
    WHERE date::date IN ('{date_filter}')
    """
    
    try:
        df_existing = pd.read_sql(query, engine)
        logger.info(f"Encontrados {len(df_existing)} registros existentes na base para essas datas")
    except Exception as e:
        logger.error(f"Erro ao buscar dados existentes: {e}")
        df_existing = pd.DataFrame()
    
    if not df_existing.empty:
        # Criar chave composta MAIS ESPECÍFICA incluindo valores numéricos
        df['composite_key'] = (
            df['portfolio_name'].astype(str) + '|' +
            df['date'].dt.strftime('%Y-%m-%d') + '|' +
            df['investor_names'].astype(str) + '|' +
            df['distributor_name'].astype(str) + '|' +
            df['account_group_names'].astype(str) + '|' +
            df['shares_amount'].round(2).astype(str) + '|' +  # Adiciona quantidade de cotas
            df['financial_value'].round(2).astype(str)  # Adiciona valor financeiro
        )
        
        df_existing['composite_key'] = (
            df_existing['portfolio_name'].astype(str) + '|' +
            pd.to_datetime(df_existing['date']).dt.strftime('%Y-%m-%d') + '|' +
            df_existing['investor_names'].astype(str) + '|' +
            df_existing['distributor_name'].astype(str) + '|' +
            df_existing['account_group_names'].astype(str) + '|' +
            df_existing['shares_amount'].round(2).astype(str) + '|' +
            df_existing['financial_value'].round(2).astype(str)
        )
        
        # Identificar registros novos
        existing_keys = set(df_existing['composite_key'].tolist())
        new_mask = ~df['composite_key'].isin(existing_keys)
        df_to_insert = df[new_mask].copy()
        
        # Remover a coluna composite_key antes de inserir
        df_to_insert = df_to_insert.drop('composite_key', axis=1)
        
        # Log de registros duplicados/atualizados
        duplicated_count = len(df) - len(df_to_insert)
        logger.info(f"Registros que já existem na base (ignorados): {duplicated_count}")
        logger.info(f"Registros novos para inserir: {len(df_to_insert)}")
        
    else:
        logger.info("Nenhum registro existente encontrado, inserindo todos os dados")
        df_to_insert = df.copy()

    # Inserir novos registros se existirem
    if len(df_to_insert) > 0:
        try:
            logger.info(f"Inserindo {len(df_to_insert)} novos registros...")
            append_to_db(df_to_insert, table_name=table_name, schema=schema)
            logger.info("✓ Inserção concluída com sucesso!")
        except Exception as e:
            logger.error(f"Erro ao inserir dados: {e}")
    else:
        logger.info("Nenhum registro novo para inserir")


def check_data_quality(df):
    """
    Verifica a qualidade dos dados antes da inserção
    """
    logger.info("Verificando qualidade dos dados...")
    
    # Verificar valores nulos em campos críticos
    critical_fields = ["portfolio_name", "date", "investor_names"]
    for field in critical_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            logger.warning(f"Campo {field} tem {null_count} valores nulos")
    
    # Verificar e remover duplicatas internas no DataFrame
    # ATUALIZADO: incluir shares_amount e financial_value na verificação
    before_count = len(df)
    duplicate_mask = df.duplicated(
        subset=[
            "portfolio_name", 
            "date", 
            "investor_names", 
            "distributor_name", 
            "account_group_names",
            "shares_amount",  # Adicionado
            "financial_value"  # Adicionado
        ],
        keep="first"
    )
    internal_duplicates = duplicate_mask.sum()
    
    if internal_duplicates > 0:
        logger.warning(f"DataFrame contém {internal_duplicates} duplicatas internas - removendo...")
        df_clean = df[~duplicate_mask].copy()
        logger.info(f"Registros após remoção de duplicatas internas: {len(df_clean)} (era {before_count})")
        return df_clean
    
    logger.info(" Qualidade dos dados OK - nenhuma duplicata interna encontrada")
    return df


def batch():
    """Execução em lote para múltiplas datas"""
    datas = pd.date_range(datetime.date(2025, 8, 30), datetime.date(2025, 8, 31))
    
    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()
    
    for date in df.date.values:
        data = tarpon_calendar.get_last_trading_day_of_month(date)   
        run(data)


def run(data=None):
    """Função principal para executar coleta de posições"""
    if data:
        logger.info("Executando o script de posições para a data %s ...", data)
    else:
        logger.info("Executando o script de posições...")

    if data is None:
        data = tarpon_calendar.get_last_trading_day_of_previous_month(datetime.date.today())

    logger.info("Buscando dados para: %s", data)

    # Carregar credenciais
    MARAVI_USER = os.getenv("MARAVI_USER")
    MARAVI_PASS = os.getenv("MARAVI_PASS")
    MARAVI_CLIENT_ID = os.getenv("MARAVI_CLIENT_ID")
    MARAVI_CLIENT_SECRET = os.getenv("MARAVI_CLIENT_SECRET")

    # Conectar na API
    logger.info("Conectando na API...")
    m = MaraviAPI(MARAVI_USER, MARAVI_PASS, MARAVI_CLIENT_ID, MARAVI_CLIENT_SECRET)
    m.authenticate()
    logger.info("Autenticado com sucesso!")

    # Preparar payload
    payload = {
        "start_date": data.strftime("%Y-%m-%d"),
        #"start_date": "2023-01-31",
        "end_date": data.strftime("%Y-%m-%d"),
        #"end_date": "2023-01-31",
        "include_participation": "true",
        "include_profitability": "true",
        "include_inactive_records": "false",
        "aggregation_mode": 6,
        "portfolio_ids": [875,1158,1159,1160,1576,1308,843,
                        427,984,144,732,506,161,964,685,499,
                        775,1298,934,1215,1299,1213,
                        657,1211,980,616,1184,1137,1277,
                        1212,1216,774,1303,159,1274,824,1569,
                        653,950,879,164,505,145,1924,1987,1539
                    ],
        "include_inactive_records": "true"
    }
    
    # Buscar dados da API
    logger.info("Buscando dados na API...")
    df = m.fetch_data("liabilities/position/get", payload)
    logger.info("Dados obtidos com sucesso!")

    # Verificar se dados foram retornados
    if df.empty:
        logger.info(f"Nenhum dado encontrado para a data: {data}")
        return
    
    # Verificar colunas obrigatórias
    required_columns = [
        "date", "shares_amount", "distributor_name", "investor_names", "financial_value",
        "portfolio_name", "participation_in_portfolio", "account_group_names", "investor_ids"
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"Colunas ausentes no DataFrame: {missing_columns}")
        logger.warning("Colunas disponíveis: " + ", ".join(df.columns.tolist()))
        return

    # Selecionar e processar colunas
    logger.info(f"Processando {len(df)} registros da API...")
    df = df[required_columns].copy()

    # Converter tipos de dados
# Converter tipos de dados
    numeric_columns = ["shares_amount", "financial_value", "participation_in_portfolio"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # ADICIONAR AQUI - Processar investor_ids
    if 'investor_ids' in df.columns:
        df['investor_ids'] = df['investor_ids'].apply(
            lambda x: int(x[0]) if isinstance(x, (list, np.ndarray)) and len(x) > 0 
            else None
        )
        df['investor_ids'] = df['investor_ids'].astype('Int64')

    # Tratar portfolio_name
    is_all_numeric = df["portfolio_name"].apply(lambda x: pd.to_numeric(x, errors='coerce')).notnull().all()
    
    if is_all_numeric:
        df["portfolio_name"] = df["portfolio_name"].astype("Int64")
    else:
        logger.info("Portfolio names contain non-numeric values, keeping as strings")
        df["portfolio_name"] = df["portfolio_name"].astype(str)

    # Verificar qualidade dos dados e remover duplicatas internas
    df_clean = check_data_quality(df)
    
    # Filtrar registros válidos
    df_positions = df_clean[df_clean["date"].notnull()].copy()
    
    logger.info(f"Registros válidos para inserção: {len(df_positions)}")
    
    # Inserir dados usando a função simplificada
    append_positions_data_simple(df_positions)
    
    logger.info(" Processo concluído!")


if __name__ == "__main__":
    run()