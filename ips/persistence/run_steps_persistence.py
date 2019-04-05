from ips.persistence.persistence import read_table_values, delete_from_table, insert_from_dataframe

RUN_STEPS_TABLE = 'RUN_STEPS'

get_steps = read_table_values(RUN_STEPS_TABLE)
delete_steps = delete_from_table(RUN_STEPS_TABLE)
create_steps = insert_from_dataframe(RUN_STEPS_TABLE, "append")

