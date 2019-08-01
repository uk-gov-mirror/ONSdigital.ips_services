import subprocess

import numpy as np
import pandas as pd
from ips.util.services_configuration import Configuration
from pkg_resources import resource_filename

from ips.util.services_logging import log
import ips.persistence.persistence as db
from ips.services.calculations import log_warnings, log_errors

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


import pandas as pd
import numpy as np


def error_check(df_survey_in, df_reference_in, run_id):
    df_survey_in.to_csv('/Users/Rushtg/survey_in.csv')
    df_reference_in.to_csv('/Users/Rushtg/reference_in.csv')

    sort1 = ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART']

    # Check data for errors before passed into R for processing
    df_survey_input_sorted = df_survey_in
    values = df_survey_input_sorted.SHIFT_WT * df_survey_input_sorted.NON_RESPONSE_WT * df_survey_input_sorted.MINS_WT * df_survey_input_sorted.TRAFFIC_WT
    df_survey_input_sorted['OOHDESIGNWEIGHT'] = values

    df_survey_input_sorted = df_survey_input_sorted[df_survey_input_sorted.OOHDESIGNWEIGHT > 0]

    df_survey_input_sorted['C_GROUP'] = np.where(df_survey_input_sorted['OOHDESIGNWEIGHT'] > 0, 1, 0)
    df_ges_input = df_survey_input_sorted[['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART', 'OOHDESIGNWEIGHT', 'C_GROUP', 'SERIAL']]
    df_ges_input = df_ges_input.sort_values(sort1)

    df_rsumsamp = df_ges_input.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART']).agg(
                                        {'OOHDESIGNWEIGHT': 'sum'}).reset_index()

    df_us_totals_check = df_reference_in
    df_us_totals_check.UNSAMP_REGION_GRP_PV.fillna(value=0, inplace=True)
    df_pop_totals = df_us_totals_check.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART'])['UNSAMP_TOTAL'].agg(
                    {'UPLIFT': 'sum'}).reset_index()

    df_prevtotals = df_survey_input_sorted.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART'])['OOHDESIGNWEIGHT'].agg(
        {'PREVTOTAL': 'sum'}).reset_index()

    df_lifted = df_prevtotals.merge(df_pop_totals, on=sort1, how='outer')
    df_lifted['PREVTOTAL'] = df_lifted['PREVTOTAL'].fillna(0)
    df_lifted['UPLIFT'] = df_lifted['UPLIFT'].fillna(0)
    uplifted = df_lifted.PREVTOTAL + df_lifted.UPLIFT
    df_lifted['UNSAMP_LIFTED'] = uplifted

    df_lifted = df_lifted[df_lifted.UNSAMP_LIFTED > 0]

    df_merge_totals = df_rsumsamp.merge(df_lifted, on=sort1, how='outer')

    # Error check 1
    df_sum_check_1 = df_merge_totals[
        df_merge_totals['OOHDESIGNWEIGHT'] > 0 & df_merge_totals['OOHDESIGNWEIGHT'].notnull()]
    df_sum_check_2 = pd.concat([df_sum_check_1[df_sum_check_1['UNSAMP_LIFTED'].isna()],
                                df_sum_check_1[df_sum_check_1['UNSAMP_LIFTED'] < 0]])

    if len(df_sum_check_2):
        error_str = "No traffic total but sampled records present for"
        for index, record in df_sum_check_2.iterrows():
            threshold_string = \
                error_str + " " + 'UNSAMP_PORT_GRP_PV' + " = " + str(record[0]) \
                + " " + 'ARRIVEDEPART' + " = " + str(record[1]) + "\n"
            log_errors(threshold_string)(pd.DataFrame(), run_id, 5)
        raise ValueError('UNSAMP_PORT_GRP_PV Failed!')

    # Error check 2
    df_sum_check_1 = df_merge_totals[df_merge_totals['UNSAMP_LIFTED'] > 0 & df_merge_totals['UNSAMP_LIFTED'].notnull()]
    df_sum_check_2 = pd.concat([df_sum_check_1[df_sum_check_1['OOHDESIGNWEIGHT'].isna()],
                                df_sum_check_1[df_sum_check_1['OOHDESIGNWEIGHT'] < 0]])

    if len(df_sum_check_2):
        error_str = "No records to match traffic against for"
        for index, record in df_sum_check_2.iterrows():
            threshold_string = \
                error_str + " " + 'UNSAMP_PORT_GRP_PV' + " = " + str(record[0]) \
                + " " + 'ARRIVEDEPART' + " = " + str(record[1]) + "\n"
            log_errors(threshold_string)(pd.DataFrame(), run_id, 5)
        raise ValueError('UNSAMP_PORT_GRP_PV Failed!')


# Prepare survey data
def r_survey_input(survey_input: pd.DataFrame) -> None:
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Creates input data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

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
def r_population_input(survey_input: pd.DataFrame, ustotals: pd.DataFrame, run_id) -> None:
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Creates population data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
                   trtotals - A data frame containing population information for
                   processing year
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    df_survey_input = survey_input
    df_us_totals = ustotals.copy()

    sort1 = ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART']

    df_survey_input_lookup = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_lookup.UNSAMP_REGION_GRP_PV.fillna(value=0, inplace=True)

    df_survey_input_lookup = df_survey_input_lookup[~df_survey_input_lookup['UNSAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_lookup = df_survey_input_lookup[~df_survey_input_lookup['ARRIVEDEPART'].isnull()]

    df_check_survey = df_survey_input_lookup.copy()
    error_check(df_check_survey, ustotals, run_id)

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
    df_survey_input = df_survey_input.round({'SHIFT_WT': 3})
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
    df_mod_totals = df_mod_totals.pivot_table(index='C_GROUP',
                                              columns='T1',
                                              values='UNSAMP_TOTAL')

    df_mod_totals = df_mod_totals.add_prefix('T_')

    # TODO: Move to persistence
    db.insert_from_dataframe('POPROWVEC_UNSAMP', if_exists='replace')(df_mod_totals)


def run_r_ges_script() -> None:
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Calls R Script to run GES Weighting
    Parameters   :
    Returns      : Writes GES output to SQL Database
    Requirements : NA
    Dependencies : NA
    """

    log.info("Starting R script.....")

    step5 = resource_filename(__name__, 'r_scripts/step5.R')

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


def do_ips_ges_weighting(df_surveydata: pd.DataFrame, df_ustotals: pd.DataFrame, run_id):
    # Deletes from poprowvec and survey_unsamp_aux tables
    db.truncate_table('SURVEY_UNSAMP_AUX')()

    db.truncate_table('POPROWVEC_UNSAMP')()
    db.truncate_table('R_UNSAMPLED')()

    # Call the GES weighting macro
    df_surveydata = df_surveydata.sort_values('SERIAL')

    r_survey_input(df_surveydata)

    r_population_input(df_surveydata, df_ustotals, run_id)

    run_r_ges_script()

    df_summarydata = db.read_table_values('R_UNSAMPLED')()
    df_summarydata = df_summarydata[['SERIAL', 'UNSAMP_TRAFFIC_WT']]
    df_summarydata['UNSAMP_TRAFFIC_WT'] = df_summarydata['UNSAMP_TRAFFIC_WT'].apply(lambda x: round(x, 3))

    return df_surveydata, df_summarydata


def do_ips_unsampled_weight_calculation(df_surveydata: pd.DataFrame, serial_num: str, shift_weight: str,
                                        nr_weight: str, min_weight: str, traffic_weight: str, out_of_hours_weight: str,
                                        df_ustotals: pd.DataFrame, min_count_threshold: int, run_id=None):
    """
    Author       : Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Performs calculations to determine the unsampled weight values
                   of the imported dataset.
    Parameters   : df_surveydata - the IPS df_surveydata records for the period                                
                   var_serialNum - variable holding the record serial number (UID)
                   var_shiftWeight - variable holding the shift weight field name                
                   var_NRWeight - variable holding the non-response weight field name        
                   var_minWeight - variable holding the minimum weight field name            
                   var_trafficWeight - variable holding the traffic weight field name        
                   df_ustotals - Population totals file
                   minCountThresh - The minimum cell count threshold
    Returns      : df_summary(dataframe containing random sample of rows)
                   df_output(dataframe containing serial number and calculated unsampled weight)
    Requirements : do_ips_ges_weighting()
    Dependencies : NA

    NOTES        : Currently GES weighing has not been written. Therefore the current solution
                   does not generate the output data frame. Once the function is written and we
                   are aware of what is being returned from the GES weighting function as well
                   as what is actually needed to be *sent* passed to the function we will rewrite the 
                   function call and implement its return functionality 
                   be rewriting the 
    """

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

    ges_dataframes = do_ips_ges_weighting(df_surveydata, df_ustotals, run_id)

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

    df_summary[PRIOR_WEIGHT_SUM_COLUMN] = df_summary[PRIOR_WEIGHT_SUM_COLUMN].round(3)
    df_summary[OOH_WEIGHT_SUM_COLUMN] = df_summary[OOH_WEIGHT_SUM_COLUMN].round(3)
    df_summary['UNSAMP_TRAFFIC_WT'] = df_summary['UNSAMP_TRAFFIC_WT'].round(3)
    df_output['UNSAMP_TRAFFIC_WT'] = df_output['UNSAMP_TRAFFIC_WT'].round(3)

    return df_output, df_summary


