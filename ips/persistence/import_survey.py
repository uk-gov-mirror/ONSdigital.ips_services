import pandas

from ips.persistence.persistence import insert_from_dataframe

SURVEY_SUBSAMPLE = 'SURVEY_SUBSAMPLE'
insert_ss = insert_from_dataframe(SURVEY_SUBSAMPLE, "append")


def import_survey_data(run_id, df):

    # Fill left side of INTDATE column with an additional 0 if length less than 8 characters
    df.columns = df.columns.str.upper()
    if 'INTDATE' in df.columns:
        df['INTDATE'] = df['INTDATE'].astype(str).str.rjust(8, '0')

    # Add the generated run id to the dataset.
    df['RUN_ID'] = pandas.Series(run_id, index=df.index)

    # Insert the imported data into the survey_subsample table on the database.
    insert_ss(df)

