import subprocess

import numpy as np
import pandas as pd
from ips.util.services_configuration import Configuration
from pkg_resources import resource_filename

from ips.util.services_logging import log
import ips.persistence.persistence as db
from ips.services.calculations import log_warnings
from ips.services.calculations.sas_rounding import ips_rounding
from pandas.testing import assert_frame_equal
import math

from ips.util.services_logging import log

from ips.persistence import data_management as idm
from ips.persistence.persistence import insert_from_dataframe, truncate_table
from ips.util.services_configuration import ServicesConfiguration
from ips.persistence.persistence import select_data

OOH_STRATA = [
    'UNSAMP_PORT_GRP_PV',
    'UNSAMP_REGION_GRP_PV',
    'ARRIVEDEPART'
]

TOTALS_COLUMN = 'UNSAMP_TOTAL'
CASE_COUNT_COLUMN = 'CASES'
OOH_WEIGHT_SUM_COLUMN = 'SUM_UNSAMP_TRAFFIC_WT'
PRIOR_WEIGHT_SUM_COLUMN = 'SUM_PRIOR_WT'
UPLIFT_COLUMN = 'UPLIFT'
PREVIOUS_TOTAL_COLUMN = 'PREVTOTAL'
POST_WEIGHT_COLUMN = 'POSTWEIGHT'


def test_unsampled():
    survey_subsample_table = 'SURVEY_SUBSAMPLE'
    config = ServicesConfiguration().get_unsampled_weight()
    run_id = 'h3re-1s-y0ur-run-1d'

    # Load survey data
    survey_data = pd.read_csv('/Users/paul/ONS/ips_services/tests/pauls scratch folder/unsampled_survey_data.csv')
    unsampled_data = pd.read_csv('/Users/paul/ONS/ips_services/tests/pauls scratch folder/unsampled_reference_data.csv')

    survey_data.drop(survey_data.columns[0], inplace=True, axis=1)
    unsampled_data.drop(unsampled_data.columns[0], inplace=True, axis=1)

    truncate_table("SAS_SURVEY_SUBSAMPLE")()
    truncate_table("PS_UNSAMPLED_OOH")()
    insert_from_dataframe('SAS_SURVEY_SUBSAMPLE')(survey_data)

    # Calculate Unsampled Weight
    output_data, summary_data = do_ips_unsampled_weight_calculation(
        df_surveydata=survey_data,
        serial_num='SERIAL',
        shift_weight='SHIFT_WT',
        nr_weight='NON_RESPONSE_WT',
        min_weight='MINS_WT',
        traffic_weight='TRAFFIC_WT',
        out_of_hours_weight="UNSAMP_TRAFFIC_WT",
        df_ustotals=unsampled_data,
        min_count_threshold=30,
        run_id=run_id)

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(output_data)
    insert_from_dataframe(config["sas_ps_table"])(summary_data)

    # Update Survey Data With Unsampled Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Unsampled Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Unsampled Weight Summary
    idm.store_step_summary(run_id, config)

    ###

    # Start testing shizznizz
    output_columns = ['SERIAL', 'UNSAMP_TRAFFIC_WT']
    summary_output_columns = ['UNSAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'UNSAMP_REGION_GRP_PV', 'CASES', 'SUM_PRIOR_WT',
                              'SUM_UNSAMP_TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT']

    # Create comparison survey dataframes
    survey_subsample = select_data("*", survey_subsample_table, "RUN_ID", run_id)

    survey_results = survey_subsample[output_columns].copy()
    survey_expected = pd.read_csv(
        "data/calculations/december_2017/unsampled_weight/surveydata_dec2017utf8.csv")
    survey_expected = survey_expected[output_columns].copy()

    survey_results.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_results.index = range(0, len(survey_results))

    survey_expected.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_expected.index = range(0, len(survey_expected))

    assert_frame_equal(survey_results, survey_expected, check_dtype=False, check_less_precise=False)

    # now the summary
    summary_data = select_data("*", 'PS_UNSAMPLED_OOH', "RUN_ID", run_id)
    summary_results = summary_data[summary_output_columns].copy()

    summary_expected = pd.read_csv(
        "data/calculations/december_2017/unsampled_weight/ps_unsampled_ooh.csv")
    summary_expected = summary_expected[summary_output_columns].copy()

    summary_results.sort_values(by=summary_output_columns, axis=0, inplace=True)
    summary_results.index = range(0, len(summary_results))

    summary_expected.sort_values(by=summary_output_columns, axis=0, inplace=True)
    summary_expected.index = range(0, len(summary_expected))

    assert_frame_equal(summary_results, summary_expected, check_dtype=False, check_less_precise=True)


# Prepare survey data
def r_survey_input(survey_input: pd.DataFrame) -> None:

    # Load survey Data
    df_survey_input = survey_input

    # Sort input values
    sort1 = ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART']

    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted.UNSAMP_REGION_GRP_PV.fillna(value=0, inplace=True)
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['UNSAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]

    # Create lookup. Group by and aggregate
    # lookup_dataframe = df_survey_input_copy
    lookup_dataframe = df_survey_input_sorted

    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['UNSAMP_PORT_GRP_PV',
                                                 'UNSAMP_REGION_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe = lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Merge lookup data in to source dataframe
    df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=['UNSAMP_PORT_GRP_PV',
                                                                              'UNSAMP_REGION_GRP_PV',
                                                                              'ARRIVEDEPART'], how='left')

    # Create traffic design weight used within GES weighting
    values = (
            df_aux_variables.SHIFT_WT
            * df_aux_variables.NON_RESPONSE_WT
            * df_aux_variables.MINS_WT
            * df_aux_variables.TRAFFIC_WT
    )

    df_aux_variables['OOH_DESIGN_WEIGHT'] = values
    df_aux_variables = df_aux_variables.sort_values(['SERIAL'])

    # Create input to pass into GES weighting
    df_r_ges_input = df_aux_variables[~df_aux_variables['T1'].isnull()]
    df_r_ges_input = df_r_ges_input[['SERIAL', 'ARRIVEDEPART', 'PORTROUTE', 'SHIFT_WT',
                                     'NON_RESPONSE_WT', 'MINS_WT', 'UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                     'OOH_DESIGN_WEIGHT', 'T1']]

    # # ROUND VALUES - Added to match SAS output
    df_r_ges_input.UNSAMP_REGION_GRP_PV = pd.to_numeric(df_r_ges_input.UNSAMP_REGION_GRP_PV, errors='coerce')

    db.insert_from_dataframe("SURVEY_UNSAMP_AUX")(df_r_ges_input)

    df_aux_variables.drop(columns=['T1', 'OOH_DESIGN_WEIGHT'], axis=1)


# Prepare population totals to create AUX lookup variables
def r_population_input(survey_input: pd.DataFrame, ustotals: pd.DataFrame) -> None:

    df_survey_input = survey_input
    df_us_totals = ustotals

    sort1 = ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART']

    df_survey_input_lookup = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_lookup.UNSAMP_REGION_GRP_PV.fillna(value=0, inplace=True)

    df_survey_input_lookup = df_survey_input_lookup[~df_survey_input_lookup['UNSAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_lookup = df_survey_input_lookup[~df_survey_input_lookup['ARRIVEDEPART'].isnull()]

    # Create lookup. Group by and aggregate. Allocates T_1 - T_n.
    lookup_dataframe = df_survey_input_lookup
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe = lookup_dataframe.replace('NOTHING', np.NaN)
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Create unsampled design weight used within GES weighting
    df_survey_input['SHIFT_WT'] = df_survey_input.SHIFT_WT.astype(np.float)

    # df_survey_input = df_survey_input.round({'SHIFT_WT': 3})
    # df_survey_input['SHIFT_WT'] = df_survey_input['SHIFT_WT'].apply(lambda x: ips_rounding(x, 3))

    values = df_survey_input.SHIFT_WT * df_survey_input.NON_RESPONSE_WT * df_survey_input.MINS_WT * df_survey_input.TRAFFIC_WT
    df_survey_input['OOH_DESIGN_WEIGHT'] = values
    df_survey_input = df_survey_input.sort_values(sort1)

    df_survey_input = df_survey_input[df_survey_input.OOH_DESIGN_WEIGHT > 0]
    df_survey_input = df_survey_input.fillna('NOTHING')

    df_prev_totals = df_survey_input.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                              'ARRIVEDEPART']).agg({"OOH_DESIGN_WEIGHT": 'sum'}).reset_index()

    df_prev_totals.rename(columns={'OOH_DESIGN_WEIGHT': 'prevtotals'}, inplace=True)
    df_prev_totals = df_prev_totals.replace('NOTHING', np.NaN)
    df_prev_totals = df_prev_totals.sort_values(sort1)

    sort1 = ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART']
    df_us_totals = df_us_totals.sort_values(sort1)
    df_us_totals = df_us_totals.fillna('NOTHING')

    df_pop_totals = df_us_totals.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                          'ARRIVEDEPART']).agg({"UNSAMP_TOTAL": 'sum'}).reset_index()

    df_pop_totals.rename(columns={'UNSAMP_TOTAL': 'uplift'}, inplace=True)
    df_pop_totals = df_pop_totals.replace('NOTHING', np.NaN)
    df_pop_totals = df_pop_totals.sort_values(sort1)

    df_pop_totals = df_pop_totals.fillna('NOTHING')
    df_prev_totals = df_prev_totals.fillna('NOTHING')

    # Merge populations totals to create one dataframe lookup
    df_lifted_totals = pd.merge(df_prev_totals, df_pop_totals, on=['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                                                   'ARRIVEDEPART'], how='left')

    df_lifted_totals = df_lifted_totals.replace('NOTHING', np.NaN)
    df_lifted_totals['uplift'] = df_lifted_totals['uplift'].fillna(0)
    df_lifted_totals = df_lifted_totals.fillna(0)

    values = df_lifted_totals.prevtotals + df_lifted_totals.uplift
    df_lifted_totals['UNSAMP_TOTAL'] = values

    df_mod_totals = pd.merge(df_lifted_totals, lookup_dataframe, on=['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                                                     'ARRIVEDEPART'], how='left')

    df_mod_totals['C_GROUP'] = 1
    df_mod_totals = df_mod_totals.drop(['ARRIVEDEPART', 'UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV'], axis=1)

    # # ROUND VALUES - Added to match SAS output
    df_mod_totals = df_mod_totals.pivot_table(index='C_GROUP', columns='T1', values='UNSAMP_TOTAL')

    df_mod_totals = df_mod_totals.add_prefix('T_')

    # TODO: Move to persistence
    db.insert_from_dataframe('POPROWVEC_UNSAMP', if_exists='replace')(df_mod_totals)


def run_r_ges_script() -> None:

    log.info("Starting R script.....")

    step5 = resource_filename(__name__, '../ips/services/calculations/r_scripts/step5.R')

    config = Configuration().cfg['database']

    username = config['user']
    password = config['password']
    database = config['database']
    server = config['server']

    subprocess.run(
        [
            "Rscript",
            "--vanilla",
            step5,
            username,
            password,
            server,
            database
        ], capture_output=False
    )

    log.info("R process finished.")


def do_ips_ges_weighting(df_surveydata: pd.DataFrame, df_ustotals: pd.DataFrame):
    # Deletes from poprowvec and survey_unsamp_aux tables
    db.truncate_table('SURVEY_UNSAMP_AUX')()

    db.truncate_table('POPROWVEC_UNSAMP')()
    db.truncate_table('R_UNSAMPLED')()

    # Call the GES weighting macro
    df_surveydata = df_surveydata.sort_values('SERIAL')

    r_survey_input(df_surveydata)

    r_population_input(df_surveydata, df_ustotals)

    run_r_ges_script()

    df_summarydata = db.read_table_values('R_UNSAMPLED')()
    df_summarydata = df_summarydata[['SERIAL', 'UNSAMP_TRAFFIC_WT']]

    return df_surveydata, df_summarydata


def do_ips_unsampled_weight_calculation(df_surveydata: pd.DataFrame, serial_num: str, shift_weight: str,
                                        nr_weight: str, min_weight: str, traffic_weight: str, out_of_hours_weight: str,
                                        df_ustotals: pd.DataFrame, min_count_threshold: int, run_id=None):

    ooh_design_weight_column = 'OOHDESIGNWEIGHT'
    # Create new column for design weights (Generate the design weights)
    df_surveydata[ooh_design_weight_column] = \
        df_surveydata[shift_weight] * df_surveydata[nr_weight] * df_surveydata[min_weight] * df_surveydata[
            traffic_weight]

    df_ustotals['REGION'] = df_ustotals['REGION'].replace(0, np.NaN)
    df_ustotals['UNSAMP_REGION_GRP_PV'] = df_ustotals['UNSAMP_REGION_GRP_PV'].replace(0, np.NaN)

    # Sort the unsampled data frame ready to be summarised
    df_ustotals = df_ustotals.sort_values(OOH_STRATA)

    # Re-index the data frame
    df_ustotals.index = range(df_ustotals.shape[0])

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.
    pop_totals = df_ustotals.fillna('NOTHING')

    # Summarise the uplift totals over the strata
    pop_totals = pop_totals.groupby(OOH_STRATA)[TOTALS_COLUMN].agg([(UPLIFT_COLUMN, 'sum')])
    pop_totals.reset_index(inplace=True)

    # Replace the previously added 'NOTHING' values with their original blank values
    pop_totals = pop_totals.replace('NOTHING', np.NaN)

    # Summarise the previous totals over the strata
    # Only use values where the OODesignWeight is greater than zero
    df_surveydata = df_surveydata.sort_values(OOH_STRATA)

    prev_totals = df_surveydata.loc[df_surveydata[ooh_design_weight_column] > 0]

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.
    prev_totals = prev_totals.fillna('NOTHING')
    prev_totals = prev_totals.groupby(OOH_STRATA)[ooh_design_weight_column].agg([(PREVIOUS_TOTAL_COLUMN, 'sum')])
    prev_totals.reset_index(inplace=True)

    # Replace the previously added 'NOTHING' values with their original blank values
    prev_totals = prev_totals.replace('NOTHING', np.NaN)

    pop_totals = pop_totals.sort_values(OOH_STRATA)

    # Generate the lifted totals data set from the two sets created

    lifted_totals = pd.merge(prev_totals, pop_totals, on=OOH_STRATA, how='left')

    # Fill blank uplift and prevtotal values with zero
    lifted_totals[UPLIFT_COLUMN].fillna(0, inplace=True)
    lifted_totals[PREVIOUS_TOTAL_COLUMN].fillna(0, inplace=True)

    # Calculate the totals column from the prevtotal and uplift values
    lifted_totals[TOTALS_COLUMN] = lifted_totals[PREVIOUS_TOTAL_COLUMN] + lifted_totals[UPLIFT_COLUMN]

    # Remove any records where var_totals value is not greater than zero
    lifted_totals = lifted_totals[lifted_totals[TOTALS_COLUMN] > 0]

    ges_dataframes = do_ips_ges_weighting(df_surveydata, df_ustotals)

    df_survey = ges_dataframes[0]
    df_output = ges_dataframes[1]
    # Sort df_surveydata dataframe before merge
    df_survey = df_survey.sort_values(serial_num)
    df_output = df_output.sort_values(serial_num)

    df_survey.index = range(0, len(df_survey))
    df_output.index = range(0, len(df_output))

    # Merge the df_surveydata and output data frame to generate the summary table
    df_survey[out_of_hours_weight] = df_output[out_of_hours_weight]

    # Fill blank UNSAMP_TRAFFIC_WT values with 1.0
    df_survey[out_of_hours_weight].fillna(1.0, inplace=True)

    # Generate POSTWEIGHT values from the UNSAMP_TRAFFIC_WT and ooh_design_weight_column values
    df_survey[POST_WEIGHT_COLUMN] = df_survey[out_of_hours_weight] * df_survey[ooh_design_weight_column]

    # Sort the data ready for summarising
    df_survey = df_survey.sort_values(OOH_STRATA)

    # Create the summary data frame from the sample with ooh_design_weight_column not equal to zero
    df_summary = df_survey[df_survey[ooh_design_weight_column] != 0]

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.
    df_summary = df_summary.fillna('NOTHING')

    # Generate a dataframe containing the count of each evaluated group
    df_cases = df_summary.groupby(OOH_STRATA)[out_of_hours_weight].agg([(CASE_COUNT_COLUMN, 'count')])

    # Flattens the column structure after adding the new CASE_COUNT_COLUMN column
    df_cases = df_cases.reset_index()

    # Summarise the data across the OOHStrataDef
    df_summary = df_summary.groupby(OOH_STRATA).agg({
        ooh_design_weight_column: 'sum',
        POST_WEIGHT_COLUMN: 'sum',
        out_of_hours_weight: 'mean'
    })

    # Flattens the column structure after adding the new ooh_design_weight_column and POSTWEIGHT columns
    df_summary = df_summary.reset_index()
    df_summary = df_summary.rename(columns={ooh_design_weight_column: PRIOR_WEIGHT_SUM_COLUMN,
                                            POST_WEIGHT_COLUMN: OOH_WEIGHT_SUM_COLUMN})

    # Merge the cases dataframe into our summary dataframe
    df_summary = pd.merge(df_summary, df_cases, on=OOH_STRATA, how='right')

    # Replace the previously added 'NOTHING' values with their original blank values
    df_summary = df_summary.replace('NOTHING', np.NaN)

    output_column_order = [
        'UNSAMP_PORT_GRP_PV',
        'UNSAMP_REGION_GRP_PV',
        'ARRIVEDEPART',
        'CASES',
        'SUM_PRIOR_WT',
        'SUM_UNSAMP_TRAFFIC_WT',
        'UNSAMP_TRAFFIC_WT']

    df_summary = df_summary[output_column_order]
    df_summary.ARRIVEDEPART = df_summary.ARRIVEDEPART.astype(int)
    # Identify groups where the total has been uplifted but the
    # respondent count is below the threshold.

    # Create unsampled data set for rows outside of the threshold
    df_unsampled_thresholds_check = (
        df_summary[(df_summary[OOH_WEIGHT_SUM_COLUMN] > df_summary[PRIOR_WEIGHT_SUM_COLUMN])
                   & (df_summary[CASE_COUNT_COLUMN] < min_count_threshold)]
    )

    # Collect data outside of specified threshold
    if len(df_unsampled_thresholds_check) > 0:
        log_warnings("Shift weight outside thresholds for")(df_unsampled_thresholds_check, 4, run_id, 5)

    # df_summary[PRIOR_WEIGHT_SUM_COLUMN] = df_summary[PRIOR_WEIGHT_SUM_COLUMN].apply(lambda x: ips_rounding(x, 3))
    # df_summary[OOH_WEIGHT_SUM_COLUMN] = df_summary[OOH_WEIGHT_SUM_COLUMN].apply(lambda x: ips_rounding(x, 10))
    # df_summary['UNSAMP_TRAFFIC_WT'] = df_summary['UNSAMP_TRAFFIC_WT'].apply(lambda x: ips_rounding(x, 3))
    # df_summary['UNSAMP_TRAFFIC_WT'] = df_summary['UNSAMP_TRAFFIC_WT'].apply(lambda x: ips_rounding(x, 3))

    return df_output, df_summary
