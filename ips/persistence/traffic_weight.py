from ips_common.config.configuration import Configuration

from ips.persistence import persistence as db

POP_PROWVEC_TABLE = 'poprowvec_traffic'
SURVEY_TRAFFIC_AUX_TABLE = "survey_traffic_aux"
R_TRAFFIC_TABLE = "r_traffic"
OUTPUT_TABLE_NAME = 'SAS_TRAFFIC_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_TRAFFIC'

save_pop_prowvec = db.insert_from_dataframe(table=POP_PROWVEC_TABLE, if_exists='replace')
read_pop_prowvec = db.read_table_values(POP_PROWVEC_TABLE)
read_r_traffic = db.read_table_values(R_TRAFFIC_TABLE)

config = Configuration().cfg['database']

username = config['user']
password = config['password']
database = config['database']
server = config['server']

truncate_survey_traffic_aux = db.truncate_table(SURVEY_TRAFFIC_AUX_TABLE)
clear_r_traffic = db.clear_memory_table(R_TRAFFIC_TABLE)
clear_pop_prowvec = db.clear_memory_table(POP_PROWVEC_TABLE)

save_sas_traffic_wt = db.insert_from_dataframe(OUTPUT_TABLE_NAME)
save_summary = db.insert_from_dataframe(SUMMARY_TABLE_NAME)


# insert dataframe into db and read back to resolve formatting issues
def convert_dataframe_to_sql_format(table_name, dataframe):
    db.insert_dataframe_into_table(table_name, dataframe)
    return db.read_table_values(table_name)()
