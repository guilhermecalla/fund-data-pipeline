import os
from sqlalchemy import create_engine
import pandas as pd


def get_engine():
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_BASE = os.getenv("DB_BASE")

    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_BASE}"
    engine = create_engine(
        conn_str,
        pool_pre_ping=True,  # <- ADICIONE
        pool_recycle=3600,   # <- ADICIONE
        echo=False)
    return engine


engine = get_engine()


def table_exists(table_name, schema):
    """Verifica se uma tabela existe no banco de dados."""
    from sqlalchemy import inspect

    inspector = inspect(engine)
    return table_name in inspector.get_table_names(schema=schema)


def get_data_from_db(table_name, schema="movimentacoes"):
    """
    Função para buscar dados de uma tabela no banco de dados PostgreSQL.

    Args:
        table_name (str): Nome da tabela a ser consultada.
        schema (str): Nome do esquema. Padrão é "movimentacoes".

    Returns:
        pd.DataFrame: DataFrame com os dados da tabela.
    """
    query = f"SELECT * FROM {schema}.{table_name}"
    df = pd.read_sql(query, engine)
    return df


def append_to_db(df, table_name, schema="movimentacoes", if_exists="append"):

    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    print("New data appended to PostgreSQL table successfully!")


def append_to_db2(df, table_name, schema="movimentacoes", if_exists="append"):
    try:
        # Tenta inserção normal
        df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    except Exception as e:
        if "duplicate key" in str(e).lower():
            # Usa SQL nativo com ON CONFLICT
            from sqlalchemy import text
            
            columns = list(df.columns)
            placeholders = ', '.join([f':{col}' for col in columns])
            columns_str = ', '.join(columns)
            
            sql = f"""
                INSERT INTO {schema}.{table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT (id) DO NOTHING
            """
            
            with engine.connect() as conn:
                conn.execute(text(sql), df.to_dict('records'))
                conn.commit()
        else:
            raise e