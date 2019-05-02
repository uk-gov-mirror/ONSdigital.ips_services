

from ips.persistence import persistence as db

POP_ROWVEC_TABLE = 'poprowvec_traffic'
SURVEY_TRAFFIC_AUX_TABLE = "survey_traffic_aux"
R_TRAFFIC_TABLE = "r_traffic"
OUTPUT_TABLE_NAME = 'SAS_TRAFFIC_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_TRAFFIC'

save_pop_rowvec = db.insert_from_dataframe(table=POP_ROWVEC_TABLE, if_exists='replace')
read_pop_rowvec = db.read_table_values(POP_ROWVEC_TABLE)

read_r_traffic = db.read_table_values(R_TRAFFIC_TABLE)

truncate_survey_traffic_aux = db.truncate_table(SURVEY_TRAFFIC_AUX_TABLE)
clear_r_traffic = db.clear_memory_table(R_TRAFFIC_TABLE)
clear_pop_prowvec = db.clear_memory_table(POP_ROWVEC_TABLE)

save_sas_traffic_wt = db.insert_from_dataframe(OUTPUT_TABLE_NAME)
save_summary = db.insert_from_dataframe(SUMMARY_TABLE_NAME)

save_survey_traffic_aux = db.insert_from_dataframe(table=SURVEY_TRAFFIC_AUX_TABLE)

