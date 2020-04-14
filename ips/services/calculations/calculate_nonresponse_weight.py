import numpy as np
import pandas as pd

from ips.util.services_logging import log

from ips.services.calculations import log_warnings, log_errors

np.seterr(all='raise')

NON_RESPONSE_DATA_TABLE_NAME = 'SAS_NON_RESPONSE_DATA'
OUTPUT_TABLE_NAME = 'SAS_NON_RESPONSE_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_NON_RESPONSE'

NON_RESPONSE_STRATA = [
    'NR_PORT_GRP_PV', 'ARRIVEDEPART'
]

SHIFTS_STRATA = [
    'NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV'
]

NR_TOTALS_COLUMN = 'MIGTOTAL'
NON_MIG_TOTALS_COLUMN = 'ORDTOTAL'
MIG_SI_COLUMN = 'MIGSI'
TAND_TSI_COLUMN = 'TANDTSI'
PSW_COLUMN = 'SHIFT_WT'
NR_FLAG_COLUMN = 'NR_FLAG_PV'
MIG_FLAG_COLUMN = 'MIG_FLAG_PV'
RESP_COUNT_COLUMN = 'COUNT_RESPS'
MEAN_SW_COLUMN = 'MEAN_RESPS_SH_WT'
PRIOR_SUM_COLUMN = 'PRIOR_SUM'
MEAN_NRW_COLUMN = 'MEAN_NR_WT'
GROSS_RESP_COLUMN = 'GROSS_RESP'
GNR_COLUMN = 'GNR'


def do_ips_nrweight_calculation(survey_data, non_response_data, non_response_weight_column, var_serial, run_id=None):
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Performs calculations to find the nonresponse weight.
    Parameters   : survey_data = the IPS survey records for the period.
                 : non_response_data = SAS data set holding migrant non-response totals and
                 : ineligible totals by strata
                 : non_response_weight_column = Variable holding the name of the non-resp. weight field
                 : var_serial = Variable holding the name of the record number field
    Returns      : df_out - dataframe containing calculated values for non_response_weight
                 : df_summary - dataframe containing a list of various columns, including the calculated non_response_wt
    Requirements : 
    Dependencies : 
    """

    # drop NON_RESPONSE_WT column in survey data at start (this matches SAS log)
    if 'NON_RESPONSE_WT' in survey_data.columns:
        survey_data = survey_data.drop(columns=['NON_RESPONSE_WT'])

    df_nonresponsedata_sorted = non_response_data.sort_values(SHIFTS_STRATA)

    survey_data['NR_PORT_GRP_PV'].fillna(0, inplace=True)
    survey_data['ARRIVEDEPART'].fillna(0, inplace=True)
    survey_data['WEEKDAY_END_PV'].fillna(0, inplace=True)
    df_surveydata_sorted = survey_data.sort_values(SHIFTS_STRATA)

    df_psw = df_surveydata_sorted.groupby(SHIFTS_STRATA)[PSW_COLUMN].agg(['mean'])
    df_psw.rename(columns={'mean': PSW_COLUMN}, inplace=True)

    # Flattens the column structure
    df_psw = df_psw.reset_index()

    # Only keep rows that exist in df_nonresponsedata_sorted 
    df_grossmignonresp = pd.merge(df_nonresponsedata_sorted, df_psw, on=SHIFTS_STRATA, how='left')

    df_grossmignonresp_err_chk = pd.merge(df_nonresponsedata_sorted, df_psw, on=SHIFTS_STRATA, how='left')

    # Add gross values using the primary sampling weight and add two new columns to df_grossmignonresp
    df_grossmignonresp['SHIFT_WT'].fillna(0, inplace=True)
    df_grossmignonresp['grossmignonresp'] = df_grossmignonresp[PSW_COLUMN] * df_grossmignonresp[NR_TOTALS_COLUMN]
    df_grossmignonresp['grossordnonresp'] = df_grossmignonresp[PSW_COLUMN] * df_grossmignonresp[NON_MIG_TOTALS_COLUMN]

    # Error Check -  Validate that non-response totals can be grossed
    df_grossmignonresp_err_chk['grossmignonresp'] = df_grossmignonresp_err_chk[PSW_COLUMN] * df_grossmignonresp_err_chk[NR_TOTALS_COLUMN]
    df_grossmignonresp_err_chk['grossordnonresp'] = df_grossmignonresp_err_chk[PSW_COLUMN] * df_grossmignonresp_err_chk[NON_MIG_TOTALS_COLUMN]

    df_migtotal_not_zero = df_grossmignonresp_err_chk[df_grossmignonresp_err_chk[NR_TOTALS_COLUMN] != 0]
    df_migtotal_error = df_migtotal_not_zero[df_migtotal_not_zero['grossmignonresp'].isnull()]

    if len(df_migtotal_error):
        error_str = "Step 2 - Unable to gross up non-response total : "
        threshold_string = ""
        for index, record in df_migtotal_error.iterrows():
            threshold_string = \
                error_str + " " + " NR_PORT_GRP_PV " + str(record[8]) \
                + " " + "ARRIVEDEPART" + " = " + str(record[3]) \
                + " " + "WEEKDAY_END_PV" + " = " + str(record[9])
            log_errors(threshold_string)(pd.DataFrame(), run_id, 2)
        raise ValueError(threshold_string)

    # Summarise over non-response strata
    df_grossmignonresp = df_grossmignonresp.sort_values(NON_RESPONSE_STRATA)
    df_summignonresp = df_grossmignonresp.groupby(NON_RESPONSE_STRATA).agg({'grossmignonresp': 'sum',
                                                                            'grossordnonresp': 'sum'})

    # Flattens the column structure after adding the new grossmignonresp and grossordnonresp columns
    df_summignonresp = df_summignonresp.reset_index()
    df_summignonresp = df_summignonresp.rename(columns={'grossordnonresp': 'grossinelresp'})

    # Calculate the grossed number of respondents over the non-response strata
    # Use only records in which NR_FLAG_PV is 0
    df_surveydata_sliced = df_surveydata_sorted.loc[df_surveydata_sorted[NR_FLAG_COLUMN] == 0]
    df_surveydata_sliced = df_surveydata_sliced.sort_values(NON_RESPONSE_STRATA)

    # Create two new columns as aggregations of SHIFT_WT
    df_sumresp = df_surveydata_sliced.groupby(NON_RESPONSE_STRATA)[PSW_COLUMN].agg(['sum', 'count'])
    df_sumresp.rename(columns={'sum': GROSS_RESP_COLUMN, 'count': RESP_COUNT_COLUMN}, inplace=True)

    # Flattens the column structure after adding the new gross_resp and count_resps columns
    df_sumresp = df_sumresp.reset_index()

    # Calculate the grossed number of T&T non-respondents of the non-response strata    
    # Use only records from the survey dataset where the NR_FLAG_PV is 1, then sort
    df_surveydata_sliced = df_surveydata_sorted.loc[df_surveydata_sorted[NR_FLAG_COLUMN] == 1]
    df_surveydata_sliced = df_surveydata_sliced.sort_values(NON_RESPONSE_STRATA)

    # Create new column using the sum of ShiftWt
    df_sumordnonresp = df_surveydata_sliced.groupby(NON_RESPONSE_STRATA)[PSW_COLUMN].agg(['sum'])
    df_sumordnonresp.rename(columns={'sum': 'grossordnonresp'}, inplace=True)

    # Flattens the column structure after adding the new grossordnonresp column
    df_sumordnonresp = df_sumordnonresp.reset_index()

    # Sort values in the three dataframes required for the next calculation
    df_sumordnonresp = df_sumordnonresp.sort_values(NON_RESPONSE_STRATA)
    df_sumresp = df_sumresp.sort_values(NON_RESPONSE_STRATA)
    df_summignonresp = df_summignonresp.sort_values(NON_RESPONSE_STRATA)

    # Use the calculated data frames to calculate the non-response weight
    # Merge previously sorted dataframes into one, ensuring all rows from summignonresp are kept
    df_gnr = df_summignonresp.merge(df_sumresp, on=NON_RESPONSE_STRATA, how='outer')
    df_gnr = df_gnr.sort_values(NON_RESPONSE_STRATA)
    df_gnr = df_gnr.merge(df_sumordnonresp, on=NON_RESPONSE_STRATA, how='left')

    # Replace all NaN values in columns with zero's
    df_gnr['grossmignonresp'].fillna(0, inplace=True)
    df_gnr['grossinelresp'].fillna(0, inplace=True)
    df_gnr['grossordnonresp'].fillna(0, inplace=True)

    # Add in two new columns with checks to prevent division by 0 
    df_gnr[GNR_COLUMN] = np.where(df_gnr[GROSS_RESP_COLUMN] != 0,
                                  df_gnr['grossordnonresp'] + df_gnr['grossmignonresp'] + df_gnr['grossinelresp'], 0)

    df_gnr[non_response_weight_column] = np.where(df_gnr[GROSS_RESP_COLUMN] != 0,
                                                  (df_gnr[GNR_COLUMN] + df_gnr[GROSS_RESP_COLUMN]) / df_gnr[
                                                      GROSS_RESP_COLUMN], np.NaN)

    df_gross_resp_is_zero = df_gnr[df_gnr[GROSS_RESP_COLUMN] == 0]

    # Collect data outside of specified threshold
    if len(df_gross_resp_is_zero):
        for index, record in df_gross_resp_is_zero.iterrows():
            error_str = "Step 2 - Gross Response is zero : "
            threshold_string = \
                error_str + " " + " NR_PORT_GRP_PV " + str(record[0]) \
                + " " + "ARRIVEDEPART" + " = " + str(record[1])
            log_errors(threshold_string)(pd.DataFrame(), run_id, 2)
        raise ValueError(threshold_string)

    # Sort df_gnr and df_surveydata ready for producing summary
    df_gnr = df_gnr.sort_values(NON_RESPONSE_STRATA)

    # Ensure only complete or partial responses are kept
    df_surveydata_sorted = df_surveydata_sorted.loc[df_surveydata_sorted[NR_FLAG_COLUMN] == 0]

    # Produce summary by merging survey data and gnr data together, then sort
    df_out = df_surveydata_sorted.merge(df_gnr[NON_RESPONSE_STRATA + [non_response_weight_column]],
                                        on=NON_RESPONSE_STRATA,
                                        how='left')

    df_out = df_out.sort_values(NON_RESPONSE_STRATA)

    # Create and add three new columns calculated using SHIFT_WT
    df_summary = df_out.groupby(SHIFTS_STRATA)[PSW_COLUMN].agg(['mean', 'count', 'sum'])

    df_summary.rename(
        columns={
            'mean': MEAN_SW_COLUMN,
            'count': RESP_COUNT_COLUMN,
            'sum': PRIOR_SUM_COLUMN
        }, inplace=True
    )

    # Flatten column structure
    df_summary.reset_index(inplace=True)

    # Create and add one new column calculated using 'non_response_wt' in a different dataframe due to difficulty in
    # creating all four new columns simultaneously in a single dataframe
    df_summary_nr = df_out.groupby(SHIFTS_STRATA)[non_response_weight_column].agg(['mean'])
    df_summary_nr.rename(columns={'mean': MEAN_NRW_COLUMN}, inplace=True)

    # Flatten column structure
    df_summary_nr.reset_index(inplace=True)

    # Merge all four new columns into the same dataframe
    df_summary = df_summary.merge(df_summary_nr, on=SHIFTS_STRATA, how='outer')

    # Merge the updated dataframe with specific columns from GNR.
    df_summary = (
        df_gnr[NON_RESPONSE_STRATA + [GNR_COLUMN, GROSS_RESP_COLUMN]].merge(
            df_summary, on=NON_RESPONSE_STRATA, how='outer'
        )
    )

    # round these to prevent truncation errors when saving to DB
    df_summary[MEAN_NRW_COLUMN] = df_summary[MEAN_NRW_COLUMN].round(3)
    df_summary[GROSS_RESP_COLUMN] = df_summary[GROSS_RESP_COLUMN].round(3)
    df_summary[MEAN_SW_COLUMN] = df_summary[MEAN_SW_COLUMN].round(3)
    df_summary[PRIOR_SUM_COLUMN] = df_summary[PRIOR_SUM_COLUMN].round(3)
    df_summary[GNR_COLUMN] = df_summary[GNR_COLUMN].round(3)

    # Calculate new non_response_wt value if condition is met
    df_out[non_response_weight_column] = (
        np.where(df_out[MIG_FLAG_COLUMN] == 0,
                 (df_out[non_response_weight_column] * df_out[TAND_TSI_COLUMN])
                 / df_out[MIG_SI_COLUMN], df_out[non_response_weight_column])
    )

    # Perform data validation
    df_count_below_threshold = df_summary[df_summary[RESP_COUNT_COLUMN] > 0]
    df_gnr_below_threshold = df_summary[df_summary[GNR_COLUMN] > 0]
    df_merged_thresholds = df_count_below_threshold.merge(df_gnr_below_threshold, how='inner')
    df_merged_thresholds = df_merged_thresholds[df_merged_thresholds[RESP_COUNT_COLUMN] < 30]
    df_merged_thresholds = df_merged_thresholds[NON_RESPONSE_STRATA]

    # Collect data outside of specified threshold
    if len(df_merged_thresholds) > 0:
        log_warnings("Respondent count below minimum threshold for")(df_merged_thresholds, 2, run_id, 2)

    # Reduce output to just key value pairs
    df_out = df_out[[var_serial, non_response_weight_column]]

    return df_out, df_summary
