import pandas

from ips.persistence.persistence import insert_from_dataframe, read_table_values

_PV_SET = 'PROCESS_VARIABLE_SET'
_insert_pv = insert_from_dataframe(_PV_SET, "append")
_get_pv = read_table_values(_PV_SET)


def create_pv_set(data):
    df = pandas.DataFrame(data, index=[0])
    _insert_pv(df)


def get_pv_sets():
    return _get_pv().to_json(orient='records')
