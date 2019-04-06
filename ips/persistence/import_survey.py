from ips.persistence.persistence import insert_from_dataframe

SURVEY_SUBSAMPLE = 'SURVEY_SUBSAMPLE'
insert_ss = insert_from_dataframe(SURVEY_SUBSAMPLE, "append")
