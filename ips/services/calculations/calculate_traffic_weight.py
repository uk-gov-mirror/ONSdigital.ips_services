import subprocess

import numpy as np
import pandas as pd
from ips_common.config.configuration import Configuration
from pkg_resources import resource_filename

from ips_common.ips_logging import log
import ips.persistence.traffic_weight as db
from ips.services.calculations import log_warnings, THRESHOLD_CAP
from ips.util.config.services_configuration import ServicesConfiguration

SERIAL = 'SERIAL'
TRAFFIC_WT = 'TRAFFIC_WT'
ARRIVEDEPART = 'ARRIVEDEPART'
T1 = "T1"

STRATA = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
MAX_RULE_LENGTH = '512'
MODEL_GROUP = 'C_group'
GES_BOUND_TYPE = 'G'
GES_UPPER_BOUND = ''
GES_LOWER_BOUND = '1.0'
GES_MAX_DIFFERENCE = '1E-8'
GES_MAX_ITERATIONS = '50'
GES_MAX_DISTANCE = '1E-8'
COUNT_COLUMN = 'CASES'
TRAFFIC_TOTAL_COLUMN = 'TRAFFICTOTAL'
POST_SUM_COLUMN = 'SUM_TRAFFIC_WT'.upper()

TRAFFIC_DESIGN_WEIGHT_COLUMN = 'TRAFDESIGNWEIGHT'
TRAFDESIGNWEIGHT = 'trafDesignWeight'

POST_WEIGHT_COLUMN = 'POSTWEIGHT'

POP_TOTALS = "SAS_TRAFFIC_DATA"
OUTPUT_TABLE_NAME = 'SAS_TRAFFIC_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_TRAFFIC'
SURVEY_TRAFFIC_AUX_TABLE = "survey_traffic_aux"
POP_ROWVEC_TABLE = 'poprowvec_traffic'
R_TRAFFIC_TABLE = "r_traffic"

var_serialNum = 'serial'.upper()
var_shiftWeight = 'shift_wt'.upper()
var_NRWeight = 'non_response_wt'.upper()
var_minWeight = 'mins_wt'.upper()
GWeightVar = 'traffic_wt'.upper()
minCountThresh = 30

SAMP_PORT_GRP_PV = 'SAMP_PORT_GRP_PV'
PORTROUTE = 'PORTROUTE'


# Prepare survey data
def r_survey_input(df_survey_input):
    """
    Author       : David Powell / edits by Nassir Mohammad
    Date         : 07/06/2018
    Purpose      : Creates input data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Sort input values
    sort1 = [SAMP_PORT_GRP_PV, ARRIVEDEPART]
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby([SAMP_PORT_GRP_PV,
                                                 ARRIVEDEPART]).agg({"count": 'count'}).reset_index()

    # Cleanse data
    # lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe[T1] = range(len(lookup_dataframe))
    lookup_dataframe[T1] = lookup_dataframe[T1] + 1

    # Merge lookup data in to source dataframe
    df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=[SAMP_PORT_GRP_PV,
                                                                              ARRIVEDEPART], how='left')

    # Create traffic design weight used within GES weighting
    values = df_aux_variables.SHIFT_WT * df_aux_variables.NON_RESPONSE_WT * df_aux_variables.MINS_WT
    df_aux_variables[TRAFDESIGNWEIGHT] = values
    df_aux_variables = df_aux_variables.sort_values([SERIAL])

    # Create input to pass into GES weighting
    df_r_ges_input = df_aux_variables[~df_aux_variables[T1].isnull()]
    df_r_ges_input[SERIAL] = df_r_ges_input.SERIAL.astype(np.float64)
    df_r_ges_input = df_r_ges_input[[SERIAL, ARRIVEDEPART, PORTROUTE, SAMP_PORT_GRP_PV, var_shiftWeight,
                                     var_NRWeight, var_minWeight, TRAFDESIGNWEIGHT, T1]]

    db.save_survey_traffic_aux(df_r_ges_input)


# Prepare population totals to create AUX lookup variables
def r_population_input(df_survey_input, df_tr_totals):
    """
    Author       : David Powell / edits by Nassir Mohammad
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

    # Sort input values
    sort1 = [SAMP_PORT_GRP_PV, ARRIVEDEPART]
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted[SAMP_PORT_GRP_PV].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted[ARRIVEDEPART].isnull()]

    # Check for errors within data before passed into R
    values = df_survey_input_sorted.SHIFT_WT * df_survey_input_sorted.NON_RESPONSE_WT * df_survey_input_sorted.MINS_WT
    df_survey_input_check = df_survey_input_sorted
    df_survey_input_check['TRAFDESIGNWEIGHT'] = values

    df_survey_input_check['C_GROUP'] = np.where(df_survey_input_check['TRAFDESIGNWEIGHT'] > 0, 1, 0)
    df_ges_input = df_survey_input_check[['SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'TRAFDESIGNWEIGHT', 'C_GROUP', 'SERIAL']]
    df_ges_input = df_ges_input.sort_values(sort1)
    df_rsumsamp = df_ges_input.groupby(['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']).agg(
        {'TRAFDESIGNWEIGHT': 'sum'}).reset_index()

    df_pop_totals_check = df_tr_totals.sort_values(sort1)
    df_pop_totals_check = df_pop_totals_check[~df_pop_totals_check['SAMP_PORT_GRP_PV'].isnull()]
    df_pop_totals_check = df_pop_totals_check[~df_pop_totals_check['ARRIVEDEPART'].isnull()]
    df_pop_totals_check = df_pop_totals_check.groupby(['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']).agg(
        {'TRAFFICTOTAL': 'sum'}).reset_index()

    df_merge_totals = df_rsumsamp.merge(df_pop_totals_check, on=sort1, how='outer')
    df_merge_totals = df_merge_totals.sort_values(sort1)

    # Error check 1
    df_sum_check_1 = df_merge_totals[
        df_merge_totals['TRAFDESIGNWEIGHT'] > 0 & df_merge_totals['TRAFDESIGNWEIGHT'].notnull()]
    df_sum_check_2 = df_sum_check_1[df_sum_check_1['TRAFFICTOTAL'] < 0 | df_sum_check_1['TRAFFICTOTAL'].isnull()]

    if len(df_sum_check_2):
        threshold_string_cap = 4000
        error_str = "No traffic total but sampled records present for"

        threshold_string = ""
        for index, record in df_sum_check_2.iterrows():
            threshold_string += \
                error_str + " " + 'SAMP_PORT_GRP_PV' + " = " + str(record[0]) \
                + " " + 'ARRIVEDEPART' + " = " + str(record[1]) + "\n"

        threshold_string_capped = threshold_string[:threshold_string_cap]
        log.error(threshold_string_capped)

    # Error check 2
    df_sum_check_1 = df_merge_totals[df_merge_totals['TRAFFICTOTAL'] > 0 & df_merge_totals['TRAFFICTOTAL'].notnull()]
    df_sum_check_2 = df_sum_check_1[
        df_sum_check_1['TRAFDESIGNWEIGHT'] < 0 | df_sum_check_1['TRAFDESIGNWEIGHT'].isnull()]

    if len(df_sum_check_2):
        threshold_string_cap = 4000
        error_str = "No records to match traffic against for"

        threshold_string = ""
        for index, record in df_sum_check_2.iterrows():
            threshold_string += \
                error_str + " " + 'SAMP_PORT_GRP_PV' + " = " + str(record[0]) \
                + " " + 'ARRIVEDEPART' + " = " + str(record[1]) + "\n"

        threshold_string_capped = threshold_string[:threshold_string_cap]
        log.error(threshold_string_capped)

    # Sort input values
    df_pop_totals = df_tr_totals.sort_values(sort1)

    # Cleanse data
    df_pop_totals = df_pop_totals[~df_pop_totals[SAMP_PORT_GRP_PV].isnull()]
    df_pop_totals = df_pop_totals[~df_pop_totals[ARRIVEDEPART].isnull()]

    # Create unique list of items from survey input
    items = df_survey_input_sorted[SAMP_PORT_GRP_PV].tolist()
    unique = []
    [unique.append(x) for x in items if x not in unique]

    df_pop_totals_match = df_pop_totals[df_pop_totals[SAMP_PORT_GRP_PV].isin(unique)]

    # Create traffic totals
    df_pop_totals_match = df_pop_totals_match.sort_values([ARRIVEDEPART, SAMP_PORT_GRP_PV])
    df_traffic_totals = df_pop_totals_match.groupby([SAMP_PORT_GRP_PV,
                                                     ARRIVEDEPART]).agg({TRAFFIC_TOTAL_COLUMN: 'sum'}).reset_index()

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby([SAMP_PORT_GRP_PV,
                                                 ARRIVEDEPART]).agg({"count": 'count'}).reset_index()

    # Cleanse data
    # lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe[T1] = range(len(lookup_dataframe))
    lookup_dataframe[T1] = lookup_dataframe[T1] + 1

    # Create population totals for current survey data - Cleanse data and merge
    lookup_dataframe_aux = lookup_dataframe[[SAMP_PORT_GRP_PV, ARRIVEDEPART, T1]]
    lookup_dataframe_aux[T1] = lookup_dataframe_aux.T1.astype(np.int64)

    df_mod_totals = pd.merge(df_traffic_totals, lookup_dataframe_aux, on=[SAMP_PORT_GRP_PV,
                                                                          ARRIVEDEPART], how='left')

    df_mod_totals[MODEL_GROUP] = 1
    df_mod_totals = df_mod_totals.drop(columns=[ARRIVEDEPART, SAMP_PORT_GRP_PV])
    df_mod_pop_totals = df_mod_totals.pivot_table(index=MODEL_GROUP,
                                                  columns=T1,
                                                  values=TRAFFIC_TOTAL_COLUMN)
    df_mod_pop_totals = df_mod_pop_totals.add_prefix('T_')

    df_mod_pop_totals[MODEL_GROUP] = 1
    cols = [MODEL_GROUP] + [col for col in df_mod_pop_totals if col != MODEL_GROUP]
    df_mod_pop_totals = df_mod_pop_totals[cols]

    df_mod_pop_totals = df_mod_pop_totals.reset_index(drop=True)

    # recreate proc_vec table

    db.save_pop_rowvec(df_mod_pop_totals)


# call R as a subprocess
def run_r_ges_script():
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

    step4 = resource_filename(__name__, 'r_scripts/step4.R')

    config = Configuration().cfg['database']

    username = config['user']
    password = config['password']
    database = config['database']
    server = config['server']

    subprocess.run(
        [
            "Rscript",
            "--vanilla",
            step4,
            username,
            password,
            server,
            database
        ], capture_output=False
    )

    log.info("R process finished.")


# generates the summary data
def generate_ips_tw_summary(df_survey, df_output_merge_final,
                            serial_um, traffic_weight,
                            pop_totals, min_count_thresh):
    """
    Author       : Nassir Mohammad
    Date         : 08 Mar 2018
    Purpose      : Calculates IPS Traffic Weight summary
    Parameters   : Survey = survey data set
                   var_serialNum = Variable holding the name of the serial number field
                   var_trafficWeight = Variable holding the name of the traffic wght field
                   var_priorWeight = Variable holding the name of the prior (design) weight
                   TrafficTotals = Traffic (population) totals dataset
                   minCountThresh = The minimum cell count threshold
    Returns      : dataframe - df_summary_merge_sum_traftot
    Requirements : TODO
    Dependencies : TODO
    """

    # #####################################################
    #
    # calculate the post weight
    # add the traffic weight to the survey data
    #
    # #####################################################

    cols_to_keep = ['serial'.upper(), 'SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'TRAFFIC_WT', 'SHIFT_WT', 'NON_RESPONSE_WT',
                    'MINS_WT', 'TRAFDESIGNWEIGHT']
    df_survey = df_survey[cols_to_keep]

    df_survey_sorted = df_survey.sort_values(serial_um)

    # drop duplicate column (with 'None' values) across both tables before merging
    df_survey_sorted_dropped = df_survey_sorted.drop(traffic_weight, 1)

    # merge tables
    df_summary_tmp = df_survey_sorted_dropped.merge(df_output_merge_final, on=serial_um, how='outer')

    # only keep rows where var_priorWeight > 0
    df_summary_tmp = df_summary_tmp[df_summary_tmp[TRAFFIC_DESIGN_WEIGHT_COLUMN] > 0]

    # calculate and add the post weight column
    df_summary_tmp[POST_WEIGHT_COLUMN] = df_summary_tmp[TRAFFIC_DESIGN_WEIGHT_COLUMN] * df_summary_tmp[
        traffic_weight]

    keep_list = [serial_um, STRATA[1],
                 STRATA[0], TRAFFIC_DESIGN_WEIGHT_COLUMN,
                 traffic_weight, POST_WEIGHT_COLUMN]

    # only keep the selected columns
    df_summary = df_summary_tmp[keep_list]

    # Summarise the results by strata
    df_summary_sorted = df_summary.sort_values(STRATA)

    # Re-index the data frame
    df_summary_sorted.index = range(df_summary_sorted.shape[0])

    # method will possibly be deprecated - may not be an issue
    df_tmp5 = (
        df_summary_sorted.groupby(STRATA).agg(
            {
                POST_WEIGHT_COLUMN: {COUNT_COLUMN: 'count', POST_SUM_COLUMN: 'sum'},
                traffic_weight: {traffic_weight: 'mean'}
            }
        )
    )

    # drop the additional column indexes
    df_tmp5.columns = df_tmp5.columns.droplevel()

    # reset indexes to keep them aligned
    df_tmp5 = df_tmp5.reset_index()

    # reorder columns for SAS comparison
    col_order = [STRATA[0], STRATA[1], COUNT_COLUMN, POST_SUM_COLUMN, traffic_weight]
    df_summary_varpostweight = df_tmp5[col_order]

    # add in the traffic totals
    df_pop_totals_stratadef_sort = pop_totals.sort_values(STRATA)

    # Re-index the data frame
    df_pop_totals_stratadef_sort.index = range(df_pop_totals_stratadef_sort.shape[0])

    df_merged = pd.merge(df_pop_totals_stratadef_sort, df_summary_varpostweight, on=STRATA, how='outer')

    df_merged[traffic_weight] = df_merged[traffic_weight].apply(lambda x: round(x, 3))
    df_merged[POST_SUM_COLUMN] = df_merged[POST_SUM_COLUMN].apply(lambda x: round(x, 3))

    # # reorder columns for SAS comparison
    col_order = [STRATA[0], STRATA[1], COUNT_COLUMN, TRAFFIC_TOTAL_COLUMN, POST_SUM_COLUMN, traffic_weight]
    df_summary_merge_sum_traftot = df_merged[col_order]

    # perform checks and log
    df_sum = df_summary_merge_sum_traftot
    df_sum_check = df_sum[(df_sum[COUNT_COLUMN].isnull()) | (df_sum[COUNT_COLUMN] < min_count_thresh)]
    df_sum_check = df_sum_check[STRATA]

    if len(df_sum_check) > 0:
        df_sum_check = df_sum_check.head(THRESHOLD_CAP)
        log_warnings("Respondent count below minimum threshold for")(df_sum_check)

    return df_summary_merge_sum_traftot


# carry out the traffic weight calculation using R call
def do_ips_trafweight_calculation_with_r(survey_data, trtotals):
    # clear the auxillary tables
    db.truncate_survey_traffic_aux()

    # drop aux tables and r created tables
    db.clear_r_traffic()
    db.clear_pop_prowvec()

    # inserts into survey_traffic_aux a.k.a. SURVEY_TRAFFIC_AUX_TABLE
    r_survey_input(survey_data)
    # inserts into POP_PROWVEC_TABLE
    r_population_input(survey_data, trtotals)

    run_r_ges_script()

    # grab the data from the SQL table and return
    output_final_import = db.read_r_traffic()

    ret_out = output_final_import[[SERIAL, TRAFFIC_WT]]

    # sort
    ret_out_sorted = ret_out.sort_values(SERIAL)
    ret_out_final = ret_out_sorted.reset_index(drop=True)

    # copy out the df without random for generate_ips_tw_summary
    df_ret_out_final_not_rounded = ret_out_final.copy()

    # Round the weights to 3dp
    ret_out_final[TRAFFIC_WT] = ret_out_final[TRAFFIC_WT].apply(lambda x: round(x, 3))

    # #################################
    # Generate the summary table
    # #################################

    # perform calculation
    survey_data[TRAFFIC_DESIGN_WEIGHT_COLUMN] = (
            survey_data[var_shiftWeight]
            * survey_data[var_NRWeight]
            * survey_data[var_minWeight]
    )

    # Summarise the population totals over the strata
    df_pop_totals = trtotals.sort_values(STRATA)

    # Re-index the data frame
    df_pop_totals.index = range(df_pop_totals.shape[0])

    df_pop_totals = (
        df_pop_totals.groupby(STRATA)[TRAFFIC_TOTAL_COLUMN].agg(
            [(TRAFFIC_TOTAL_COLUMN, 'sum')]
        ).reset_index()
    )

    # ensure un-rounded df_ret_out_final_not_rounded is supplied
    df_summary_merge_sum_traftot = generate_ips_tw_summary(
        survey_data,
        df_ret_out_final_not_rounded,
        var_serialNum,
        GWeightVar,
        df_pop_totals,
        minCountThresh
    )

    # update the output SQL tables
    db.save_sas_traffic_wt(ret_out_final)
    db.save_summary(df_summary_merge_sum_traftot)

    return ret_out_final, df_summary_merge_sum_traftot
