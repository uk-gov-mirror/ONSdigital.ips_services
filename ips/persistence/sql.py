import traceback
from typing import Optional

import pandas
import sqlalchemy
from ips.util.services_configuration import Configuration
from ips.util.services_logging import log

eng = None

config = Configuration().cfg['database']

username = config['user']
password = config['password']
database = config['database']
server = config['server']

connection_string = f"mysql+pymysql://{username}:{password}@{server}/{database}"


def get_sql_connection():
    global eng

    if eng is not None:
        return eng.connect()

    # Get credentials and decrypt

    try:
        engi = sqlalchemy.create_engine(connection_string)
        eng = engi
        return engi.connect()
    except Exception as err:
        log.error(f"get_sql_connection failed: {err}")
        raise err


def drop_table(table_name: str) -> None:
    conn = None
    try:
        conn = get_sql_connection()
        conn.execute("DROP TABLE IF EXISTS " + table_name)
    except Exception as err:
        log.error(f"drop_table failed: {err}")
        raise err
    finally:
        if conn is not None:
            conn.close()


def delete_from_table(table_name: str, condition1: str = None, operator: str = None,
                      condition2: str = None, condition3: str = None) -> None:
    if condition1 is None:
        query = ("DELETE FROM " + table_name)
    elif condition3 is None:
        query = ("DELETE FROM " + table_name
                 + " WHERE " + condition1
                 + " " + operator
                 + " '" + condition2 + "'")
    else:
        query = ("DELETE FROM " + table_name
                 + " WHERE " + condition1
                 + " " + operator
                 + " '" + condition2 + "'"
                 + " AND " + condition3)

    conn = None
    try:
        conn = get_sql_connection()
        conn.execute(query)
    except Exception as err:
        traceback.print_exc()
        log.error(f"delete_from_table failed: {err}")
        raise err
    finally:
        if conn is not None:
            conn.close()


def select_data(column_name: str, table_name: str, condition1: str, condition2: str) -> Optional[pandas.DataFrame]:
    query = f"""
        SELECT {column_name} 
        FROM {table_name}
        WHERE {condition1} = '{condition2}'
        """
    conn = None
    try:
        conn = get_sql_connection()
        return pandas.read_sql_query(query, con=conn)
    except Exception as err:
        log.error(f"select_data failed: {err}")
        raise err
    finally:
        if conn is not None:
            conn.close()


def get_table_values(table_name: str) -> pandas.DataFrame:
    conn = None
    try:
        conn = get_sql_connection()
        return pandas.read_sql_table(table_name=table_name, con=conn)
    except Exception as err:
        log.error(f"get_table_values failed: {err}")
        raise err
    finally:
        if conn is not None:
            conn.close()


def insert_json_into_table(table: str, data: str, if_exists='append'):
    df = pandas.DataFrame(data, index=[0])

    # Writes data frame to sql
    insert_dataframe_into_table(table, df, if_exists)


def insert_dataframe_into_table(table_name: str,
                                dataframe: pandas.DataFrame,
                                if_exists='append') -> None:
    dataframe = dataframe.where((pandas.notnull(dataframe)), None)
    dataframe.columns = dataframe.columns.astype(str)

    conn = None
    try:
        conn = get_sql_connection()
        dataframe.to_sql(table_name, con=conn, if_exists=if_exists,
                         chunksize=5000, index=False)
    except Exception as err:
        log.error(f"insert_dataframe_into_table failed: {err}")
        raise err
    finally:
        if conn is not None:
            conn.close()


def execute_sql_statement(sq):
    conn = None
    try:
        conn = get_sql_connection()
        return conn.execute(sq)
    except Exception as err:
        log.error(f"execute_sql_statement failed: {err}")
        raise err
    finally:
        if conn is not None:
            conn.close()


def execute_sql_statement_id(sq):
    conn = None
    try:
        conn = get_sql_connection()
        conn.execute(sq)
        return conn.execute("SELECT @@IDENTITY AS id")
    except Exception as err:
        log.error(f"execute_sql_statement failed: {err}")
        raise err
    finally:
        if conn is not None:
            conn.close()
