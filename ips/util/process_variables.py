import multiprocessing

from functools import partial
import pandas
from ips.util.services_logging import log
import ips.persistence.sql as db
import numpy as np
# for exec
# noinspection PyUnresolvedReferences
import math
# for exec
# noinspection PyUnresolvedReferences
from datetime import datetime

from ips.persistence.persistence import insert_from_dataframe
from RestrictedPython import compile_restricted
from RestrictedPython import safe_builtins


def getitem(object, name, default=None):  # known special case of getitem
    """
    extend to ensure only specific items can be accessed if required
    """
    # raise RestrictedException("You bad boy!")
    return object[name]


def _write_wrapper():
    # Construct the write wrapper class
    def _handler(secattr, error_msg):
        # Make a class method.
        def handler(self, *args):
            try:
                f = getattr(self.ob, secattr)
            except AttributeError:
                raise TypeError(error_msg)
            f(*args)

        return handler

    class Wrapper(object):
        def __init__(self, ob):
            self.__dict__['ob'] = ob

        __setitem__ = _handler(
            '__guarded_setitem__',
            'object does not support item or slice assignment')

        __delitem__ = _handler(
            '__guarded_delitem__',
            'object does not support item or slice assignment')

        __setattr__ = _handler(
            '__guarded_setattr__',
            'attribute-less object (assign or del)')

        __delattr__ = _handler(
            '__guarded_delattr__',
            'attribute-less object (assign or del)')

    return Wrapper


def _write_guard():
    # Nested scope abuse!
    # safetypes and Wrapper variables are used by guard()
    safetypes = {dict, list, pandas.DataFrame, pandas.Series}
    Wrapper = _write_wrapper()

    def guard(ob):
        # Don't bother wrapping simple types, or objects that claim to
        # handle their own write security.
        if type(ob) in safetypes or hasattr(ob, '_guarded_writes'):
            return ob
        # Hand the object to the Wrapper instance, then return the instance.
        return Wrapper(ob)

    return guard


write_guard = _write_guard()

safe_globals = dict(__builtins__=safe_builtins)

safe_builtins['_getitem_'] = getitem
safe_builtins['_getattr_'] = getattr
safe_builtins['_write_'] = write_guard
safe_builtins['math'] = math
safe_builtins['datetime'] = datetime


def modify_values(row, dataset, pvs):
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

    for pv in pvs:
        safe_builtins['row'] = row
        safe_builtins['dataset'] = dataset
        code = pv['PROCVAR_RULE']
        try:
            exec(code, safe_globals, None)
        except ValueError:
            name = pv['PROCVAR_NAME']
            log.error(f"ValueError on PV: {name}")
            raise ValueError

        except KeyError:
            name = pv['PROCVAR_NAME']
            log.error(f"KeyError on PV: {name}")
            raise KeyError

        except TypeError:
            name = pv['PROCVAR_NAME']
            log.error(f"TypeError on PV: {name}")
            raise TypeError

        except SyntaxError:
            name = pv['PROCVAR_NAME']
            log.error(f"SyntaxError on PV: {name}")
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
        sql = "SELECT PROCVAR_NAME,PROCVAR_RULE FROM  SAS_PROCESS_VARIABLE ORDER BY  PROCVAR_ORDER"
        v = conn.engine.execute(sql)
        return v.fetchall()


def parallel_func(pv_df, pv_list, dataset=None):
    compile_pvs(pv_list)
    return pv_df.apply(modify_values, axis=1, args=(dataset, pv_list))


def compile_pvs(pv_list):
    for a in pv_list:
        a['PROCVAR_RULE'] = compile_restricted(
            a['PROCVAR_RULE'],
            filename=a['PROCVAR_NAME'],
            mode='exec'
        )


def parallelise_pvs(dataframe, process_variables, dataset=None):
    num_partitions = multiprocessing.cpu_count()
    df_split = np.array_split(dataframe, num_partitions)
    pool = multiprocessing.Pool(num_partitions)

    res = pandas.concat(
        pool.map(
            partial(parallel_func, pv_list=process_variables, dataset=dataset),
            df_split
        ),
        sort=True
    )

    pool.close()
    pool.join()

    return res


def process(in_table_name, out_table_name, in_id, dataset):
    # Ensure the input table name is capitalised
    in_table_name = in_table_name.upper()

    # Extract the table's content into a local dataframe
    df_data = db.get_table_values(in_table_name)

    # Fill nan values
    df_data.fillna(value=np.NaN, inplace=True)

    # Get the process variable statements
    process_variables = get_pvs()

    pvs = []
    for a in process_variables:
        c = dict(a.items())
        pvs.append(c)

    if dataset == 'survey':
        df_data = df_data.sort_values('SERIAL')

    df_data = parallelise_pvs(df_data, pvs, dataset)

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

    # Insert the dataframe to the output table
    insert_from_dataframe(out_table_name)(df_out)
