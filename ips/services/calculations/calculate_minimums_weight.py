import numpy as np
import pandas as pd

from ips_common.ips_logging import log

from ips.services.calculations import log_warnings

OUTPUT_TABLE_NAME = 'SAS_MINIMUMS_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_MINIMUMS'
STRATA = ['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV']
MINIMUM_COUNT_COLUMN = 'MINS_CASES'
FULL_RESPONSE_COUNT_COLUMN = 'FULLS_CASES'
MINIMUM_FLAG_COLUMN = 'MINS_FLAG_PV'
PRIOR_WEIGHT_MINIMUM_COLUMN = 'PRIOR_GROSS_MINS'
PRIOR_WEIGHT_FULL_COLUMN = 'PRIOR_GROSS_FULLS'
PRIOR_WEIGHT_ALL_COLUMN = 'PRIOR_GROSS_ALL'
POST_WEIGHT_COLUMN = 'POST_SUM'
CASES_CARRIED_FORWARD_COLUMN = 'CASES_CARRIED_FWD'


def do_ips_minweight_calculation(df_surveydata, serial_num, shift_weight, nr_weight, min_weight):
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Performs the calculation of minimums weights
    Parameters   : df_surveydata - dataframe containing the survey data
                 : var_serialNum - name of the column containing serial number
                 : var_shiftWeight - name of the column containing calculated shift_wt values
                 : var_NRWeight - name of the column containing calculated non_response_wt values
                 : var_minWeight - name of the column to contain calculated min_wt values
    Returns      : df_out, containing a list of serial numbers with the corresponding calculated mins_wt values
                 : df_summary, containing a summary of supporting variables related to mins_wt.
    Requirements : 
    Dependencies :
    """

    df_surveydata_new = df_surveydata[df_surveydata[shift_weight].notnull()]

    df_surveydata_new = df_surveydata_new[df_surveydata_new[nr_weight].notnull()]

    df_surveydata_new["MINS_CTRY_GRP_PV"].fillna(0, inplace=True)

    df_surveydata_new['SWNRwght'] = df_surveydata_new[shift_weight] * df_surveydata_new[nr_weight]

    df_surveydata_sorted = df_surveydata_new.sort_values(STRATA)

    # Summarise the minimum responses by the strata
    df_mins = df_surveydata_sorted[df_surveydata_sorted[MINIMUM_FLAG_COLUMN] == 1]

    df_mins.reset_index(inplace=True)

    df_summin = df_mins.groupby(STRATA)['SWNRwght'].agg(['sum', 'count'])
    df_summin.rename(columns={'sum': PRIOR_WEIGHT_MINIMUM_COLUMN, 'count': MINIMUM_COUNT_COLUMN}, inplace=True)

    df_summin.reset_index(inplace=True)

    # Summarise only full responses by strata
    df_fulls = df_surveydata_sorted[df_surveydata_sorted[MINIMUM_FLAG_COLUMN] == 0]

    df_sumfull = df_fulls.groupby(STRATA)['SWNRwght'].agg(['sum', 'count'])
    df_sumfull.rename(columns={'sum': PRIOR_WEIGHT_FULL_COLUMN, 'count': FULL_RESPONSE_COUNT_COLUMN}, inplace=True)

    df_sumfull.reset_index(inplace=True)

    # Summarise the mig slot interviews by the strata
    df_migs = df_surveydata_sorted[df_surveydata_sorted[MINIMUM_FLAG_COLUMN] == 2]

    df_summig = df_migs.groupby(STRATA)['SWNRwght'].agg(['sum'])
    df_summig.rename(columns={'sum': 'sumPriorWeightMigs'}, inplace=True)

    df_summig.reset_index(inplace=True)

    # Calculate the minimum weight by the strata
    df_summin.sort_values(STRATA)
    df_sumfull.sort_values(STRATA)
    df_summig.sort_values(STRATA)

    df_summary = pd.merge(df_sumfull, df_summig, on=STRATA, how='outer')

    df_summary = df_summary.merge(df_summin, on=STRATA, how='outer')

    df_check_prior_gross_fulls = df_summary[df_summary[PRIOR_WEIGHT_FULL_COLUMN] <= 0]

    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_check_prior_gross_fulls.iterrows():
        threshold_string += "___||___" \
                            + df_check_prior_gross_fulls.columns[0] + " : " + str(record[0])

    if not df_check_prior_gross_fulls.empty and not df_summig.empty:
        log.error('Error: No complete or partial responses' + threshold_string)
    else:
        df_summary[min_weight] = np.where(df_summary[PRIOR_WEIGHT_FULL_COLUMN] > 0,
                                          (df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN] +
                                           df_summary[PRIOR_WEIGHT_FULL_COLUMN]) /
                                          df_summary[PRIOR_WEIGHT_FULL_COLUMN],
                                          1)

    # Replace missing values with 0
    df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN].fillna(0, inplace=True)
    df_summary[PRIOR_WEIGHT_FULL_COLUMN].fillna(0, inplace=True)
    df_summary["sumPriorWeightMigs"].fillna(0, inplace=True)

    df_summary[PRIOR_WEIGHT_ALL_COLUMN] = (
            df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN]
            + df_summary[PRIOR_WEIGHT_FULL_COLUMN]
            + df_summary["sumPriorWeightMigs"])

    df_summary = df_summary.sort_values(STRATA)

    df_summary[min_weight] = np.where(df_summary[PRIOR_WEIGHT_FULL_COLUMN] > 0,
                                      ((df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN] +
                                        df_summary[PRIOR_WEIGHT_FULL_COLUMN]) / df_summary[
                                           PRIOR_WEIGHT_FULL_COLUMN]),
                                      df_summary[min_weight])

    df_surveydata_sorted.fillna(0, inplace=True)

    # This merge creates two mins_wt columns, x and y/
    df_out = df_summary.merge(df_surveydata_sorted, on=STRATA, how='outer')

    # Remove empty mins_wt_y column and rename mins_wt_x to mins_wt
    df_out = df_out.drop(min_weight + '_y', axis=1)

    df_out.rename(index=str, columns={min_weight + '_x': min_weight}, inplace=True)

    df_out.sort_values(serial_num)

    df_test_pre = pd.DataFrame(columns=[min_weight, MINIMUM_FLAG_COLUMN])

    df_test_post_1 = pd.DataFrame(columns=[min_weight, MINIMUM_FLAG_COLUMN])

    df_test_post_2 = pd.DataFrame(columns=[min_weight, MINIMUM_FLAG_COLUMN])

    df_test_pre[min_weight] = df_out[min_weight]

    df_test_pre[MINIMUM_FLAG_COLUMN] = df_out[MINIMUM_FLAG_COLUMN]

    # Set mins_wt to either 0 or 1 conditionally, then calculate the postweight value
    df_out[min_weight] = np.where(df_out[MINIMUM_FLAG_COLUMN] == 1.0, 0, df_out[min_weight])

    df_test_post_1[min_weight] = df_out[min_weight]

    df_test_post_1[MINIMUM_FLAG_COLUMN] = df_out[MINIMUM_FLAG_COLUMN]

    df_out[min_weight] = np.where(df_out[MINIMUM_FLAG_COLUMN] == 2.0, 1, df_out[min_weight])

    df_test_post_2[min_weight] = df_out[min_weight]

    df_test_post_2[MINIMUM_FLAG_COLUMN] = df_out[MINIMUM_FLAG_COLUMN]

    df_out['SWNRMINwght'] = (
            df_out[shift_weight]
            * df_out[nr_weight]
            * df_out[min_weight]
    )

    df_out_sliced = df_out[df_out[MINIMUM_FLAG_COLUMN] != 1]
    df_postsum = df_out_sliced.groupby(STRATA)['SWNRMINwght'].agg(['sum', 'count'])
    df_postsum.rename(columns={'sum': POST_WEIGHT_COLUMN, 'count': CASES_CARRIED_FORWARD_COLUMN}, inplace=True)

    df_postsum.reset_index(inplace=True)

    df_postsum.sort_values(STRATA)

    # Merge the updated dataframe with specific columns from GNR.
    df_summary = df_summary.merge(df_postsum, on=STRATA, how='outer')

    df_summary.drop(["sumPriorWeightMigs"], axis=1, inplace=True)

    df_summary.sort_values(STRATA, inplace=True)

    # Perform data validation
    df_fulls_below_threshold = df_summary[df_summary[FULL_RESPONSE_COUNT_COLUMN] < 30]
    df_mins_below_threshold = df_summary[df_summary[MINIMUM_COUNT_COLUMN] > 0]

    df_merged_thresholds = df_fulls_below_threshold.merge(df_mins_below_threshold, how='inner')
    df_merged_thresholds = df_merged_thresholds[STRATA]

    # Collect data outside of specified threshold
    if len(df_merged_thresholds) > 0:
        log_warnings("Minimums weight outside thresholds for")(df_merged_thresholds)

    df_out = df_out[[serial_num, min_weight]]

    # This block of rounding was largely used to test and to bring the results closer in line with the SAS results.
    # They can be removed if desired in order to produce a new standard test set.
    df_out[min_weight] = df_out[min_weight].round(3)
    columns_to_round = [PRIOR_WEIGHT_ALL_COLUMN, PRIOR_WEIGHT_FULL_COLUMN, PRIOR_WEIGHT_MINIMUM_COLUMN, min_weight,
                        POST_WEIGHT_COLUMN]
    df_summary[columns_to_round] = df_summary[columns_to_round].round(3)

    df_out = df_out.sort_values(serial_num)

    df_summary["MINS_CTRY_GRP_PV"] = df_summary["MINS_CTRY_GRP_PV"].replace(0, float('nan'))

    return df_out, df_summary
