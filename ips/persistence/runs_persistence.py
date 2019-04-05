from ips.persistence.persistence import read_table_values, delete_from_table, insert_from_json

RUNS_TABLE = 'RUN'

get_runs = read_table_values(RUNS_TABLE)
delete_run = delete_from_table(RUNS_TABLE)
create_run_data = insert_from_json(RUNS_TABLE, "append")
insert_run = insert_from_json(RUNS_TABLE, "append")
