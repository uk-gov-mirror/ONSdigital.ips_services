from ips.persistence.persistence import read_table_values, delete_from_table, insert_from_dataframe

PROCESS_VARIABLES_TABLE = 'PROCESS_VARIABLE_PY'

get_pv = read_table_values(PROCESS_VARIABLES_TABLE)
delete_pv = delete_from_table(PROCESS_VARIABLES_TABLE)
insert_pv = insert_from_dataframe(PROCESS_VARIABLES_TABLE, "append")
