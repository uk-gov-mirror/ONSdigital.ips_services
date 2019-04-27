import multiprocessing
import random
from functools import partial

import pandas
from ips_common.ips_logging import logging as log
import ips_common_db.sql as db
import numpy as np
# for exec
import math

random.seed(123456)

count = 1

# NOTE: THis will not work on non Unix like systems due to the way fork() works
compiled_pv_list = []
pv_name = []


def modify_values(row, dataset):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Applies the PV rules to the specified dataframe on a row by row basis.
    Parameters   : row - the row of a dataframe passed to the function through the 'apply' statement called
                   pvs - a collection of pv names and statements to be applied to the dataframe's rows.
                   dataset -  and identifier used in the executed pv statements.
    Returns      : a modified row to be reinserted into the dataframe.
    Requirements : this function must be called through a pandas apply statement.
    Dependencies : NA
    """

    for code in compiled_pv_list:
        try:
            exec(code)
        except ValueError:
            log.error(f"ValueError on PV: {pv_name[compiled_pv_list.index(code)]}")
            raise ValueError

        except KeyError:
            log.error(f"KeyError on PV: {pv_name[compiled_pv_list.index(code)]}")
            raise KeyError

        except TypeError:
            log.error(f"TypeError on PV: {pv_name[compiled_pv_list.index(code)]}")
            raise TypeError

        except SyntaxError:
            log.error(f"SyntaxError on PV: {pv_name[compiled_pv_list.index(code)]}")
            raise SyntaxError

    if dataset in ('survey', 'shift'):
        row['SHIFT_PORT_GRP_PV'] = str(row['SHIFT_PORT_GRP_PV'])[:10]

    return row


def get_pvs():
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Extracts the PV data from the process_variables table.
    Parameters   : conn - a connection object linking  the database.
    Returns      : a collection of pv names and statements
    Requirements : NA
    Dependencies : NA
    """

    engine = db.get_sql_connection()

    if engine is None:
        raise ConnectionError("Cannot get database connection")

    with engine.connect() as conn:
        sql = """SELECT 
                    PROCVAR_NAME,PROCVAR_RULE
                 FROM 
                    SAS_PROCESS_VARIABLE
                 ORDER BY 
                    PROCVAR_ORDER"""

        v = conn.engine.execute(sql)
        return v.fetchall()


def parallel_func(pv_df, dataset=None):
    return pv_df.apply(modify_values, axis=1, args=(dataset,))


def compile_pvs(pv_list):
    global compiled_pv_list
    global pv_name

    compiled_pv_list.clear()
    pv_name.clear()

    for pv in pv_list:
        compiled_pv_list.append(compile(pv[1], 'pv', 'exec'))
        pv_name.append(pv[0])


def parallelise_pvs(dataframe, dataset=None):
    # return parallel_func(dataframe, dataset)

    num_partitions = multiprocessing.cpu_count()
    df_split = np.array_split(dataframe, num_partitions)
    pool = multiprocessing.Pool(num_partitions)

    res = pandas.concat(
        pool.map(
            partial(parallel_func, dataset=dataset),
            df_split
        ),
        sort=True
    )

    pool.close()
    pool.join()

    return res


def process(in_table_name, out_table_name, in_id, dataset):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Runs the process variables step of the IPS calculation process.
    Parameters   : in_table_name - the table where the data is coming from.
                   out_table_name - the destination table where the modified data will be sent.
                   in_id - the column id used in the output dataset (this is used when the data is merged into the main
                           table later.
                   dataset - an identifier for the dataset currently being processed.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Ensure the input table name is capitalised
    in_table_name = in_table_name.upper()

    # Extract the table's content into a local dataframe
    df_data = db.get_table_values(in_table_name)

    # Fill nan values
    df_data.fillna(value=np.NaN, inplace=True)

    # Get the process variable statements
    process_variables = get_pvs()
    compile_pvs(process_variables)

    if dataset == 'survey':
        df_data = df_data.sort_values('SERIAL')

    # Apply process variables
    # df_data = df_data.apply(modify_values, axis=1, args=(process_variables, dataset))
    df_data = parallelise_pvs(df_data, dataset)

    # Create a list to hold the PV column names
    updated_columns = []

    # Loop through the pv's
    for pv in process_variables:
        updated_columns.append(pv[0].upper())

    # Generate a column list from the in_id column and the pvs for the current run
    columns = [in_id] + updated_columns
    columns = [col.upper() for col in columns]
    # Create a new dataframe from the modified data using the columns specified
    df_out = df_data[columns]

    # for column in df_out:
    #     if df_out[column].dtype == np.int64:
    #         df_out[column] = df_out[column].astype(int)

    # Insert the dataframe to the output table
    db.insert_dataframe_into_table(out_table_name, df_out)
