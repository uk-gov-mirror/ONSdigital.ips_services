from typing import Callable, Any

import ips_common_db.sql as db
import pandas
import pandas as pd
from ips_common.ips_logging import log


def clear_memory_table(table: str) -> Callable[[str], None]:
    def clear():
        db.clear_memory_table(table)

    return clear


def read_table_values(table: str) -> Callable[[], pd.DataFrame]:
    """
        A closure that takes the name of a table to read from.
    :param table: the name of the table to read from
    :return: a function that, when called will return a pandas DataFrame containing all rows in the table
    """

    def read():
        return db.get_table_values(table)

    return read


def truncate_table(table: str) -> Callable[[str], None]:

    def truncate():
        db.execute_sql_statement(f"TRUNCATE TABLE {table}")

    return truncate


def delete_from_table(table: str) -> Callable[..., None]:
    """
        A closure that takes the name of a table to delete from and returns a function for that table that can be
        called with any number of key/values that are mapped to table columes and their values as delete predicates

    :param table: the name of the table to delete all run_id  rows from
    :return: a function that is called with the run_id
    """

    def delete(**kwargs):
        if len(kwargs) == 0:
            val = f"TRUNCATE {table}"
        else:
            val = f"DELETE FROM {table}"

            if len(kwargs) == 1:
                key, value = kwargs.popitem()
                if isinstance(value, str):
                    value = '"' + value + '"'
                val += f' WHERE {key} = {value}'
            else:
                i = 0
                for key, value in kwargs.items():
                    if isinstance(value, str):
                        value = '"' + value + '"'
                    if i == 0:
                        val += f' WHERE {key} = {value}'
                    else:
                        val += f' {key} = {value}'
                    i = i + 1
                    if i != len(kwargs):
                        val += ' AND '

        log.debug(val)

        db.execute_sql_statement(val)

    return delete


def insert_into_table(table: str) -> Callable[..., None]:
    """
        A closure that takes the name of a table to insert into. and returns a function for that table that can be
        called with any number of key/values that are mapped to table columes and their values as select predicates

    :param table: the name of the table to insert
    :return: a function that, when called will insert key/value items into the table
    """

    def insert(**kwargs):
        val = f"INSERT INTO {table} ("
        if len(kwargs) == 1:
            key, value = kwargs.popitem()
            if isinstance(value, str):
                value = '"' + value + '"'
            val += f' {key}) VALUES({value})'
        else:
            i = 0
            for key, _ in kwargs.items():
                val += key
                i = i + 1
                if i != len(kwargs):
                    val += ', '
                else:
                    val += ') VALUES ('
            i = 0
            for _, value in kwargs.items():
                if isinstance(value, str):
                    value = '"' + value + '"'
                val += value
                i = i + 1
                if i != len(kwargs):
                    val += ', '
                else:
                    val += ') '

        log.debug(val)
        db.execute_sql_statement(val)

    return insert


def insert_into_table_id(table: str) -> Callable[..., None]:
    """
        A closure that takes the name of a table to insert into. and returns a function for that table that can be
        called with any number of key/values that are mapped to table columes and their values as select predicates
    :param table: the name of the table to insert
    :return: a function that, when called will insert key/value items into the table
    """

    def insert(**kwargs):
        val = f"INSERT INTO {table} ("
        if len(kwargs) == 1:
            key, value = kwargs.popitem()
            if isinstance(value, str):
                value = '"' + value + '"'
            val += f' {key}) VALUES({value})'
        else:
            i = 0
            for key, _ in kwargs.items():
                val += key
                i = i + 1
                if i != len(kwargs):
                    val += ', '
                else:
                    val += ') VALUES ('
            i = 0
            for _, value in kwargs.items():
                if isinstance(value, str):
                    value = '"' + value + '"'
                val += value
                i = i + 1
                if i != len(kwargs):
                    val += ', '
                else:
                    val += ') '

        log.debug(val)
        return db.execute_sql_statement_id(val)

    return insert


def insert_from_dataframe(table: str, if_exists: str = "append", index=False) -> Callable[[pd.DataFrame], None]:

    def insert(d: pd.DataFrame):
        insert_dataframe_into_table(table, d, if_exists, index)

    return insert


def insert_dataframe_into_table(table_name: str,
                                dataframe: pandas.DataFrame,
                                if_exists='append',
                                index=False) -> None:

    try:
        dataframe.to_sql(table_name, con=db.connection_string, if_exists=if_exists,
                         chunksize=5000, index=index)
    except Exception as err:
        log.error(f"insert_dataframe_into_table failed: {err}")
        raise err


def insert_from_json(table: str, if_exists: str = "append") -> Callable[[str], None]:
    """
        A closure that inserts a json into a table by converting the json into a Dataframe and inserting
    :param table: the name of the table to insert into
    :param if_exists: the pandas if_exists string. One of 'append|fail|replace'
    :return: a function that when called with a DataFrame, inserts said DataFrame into the table
    """

    def insert(data: str):
        data_frame = pd.DataFrame(data, index=[0])
        db.insert_dataframe_into_table(table, data_frame, if_exists)

    return insert


def select_data(column_name: str, table: str, condition1: str, condition2: str):
    return db.select_data(column_name, table, condition1, condition2)


def execute_sql() -> Callable[[str], Any]:
    def execute(sql: str):
        return db.execute_sql_statement(sql)

    return execute


def get_identity(table: str, id_column: str) -> str:
    return str(
        db.execute_sql_statement(f"SELECT {id_column} FROM {table} ORDER BY {id_column} DESC LIMIT 1").first()[0])

def execute_sql_statement_id(sq):
    execute_sql()(sq)
    return execute_sql()("SELECT @@IDENTITY AS id")