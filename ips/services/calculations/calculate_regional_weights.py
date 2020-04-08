from ips.services.calculations.sas_rounding import ips_rounding
import math
import numpy as np
import pandas as pd

OUTPUT_TABLE_NAME = 'SAS_REGIONAL_IMP'
MAXIMUM_LEVEL = 4
STAY_VARIABLE = 'STAY'
SPEND_VARIABLE = 'SPEND'
STAY_WEIGHT_VARIABLE = 'STAY_WT'
VISIT_WEIGHT_VARIABLE = 'VISIT_WT'
EXPENDITURE_WEIGHT_VARIABLE = 'EXPENDITURE_WT'
STAY_WEIGHTK_VARIABLE = 'STAY_WTK'
VISIT_WEIGHTK_VARIABLE = 'VISIT_WTK'
EXPENDITURE_WEIGHTK_VARIABLE = 'EXPENDITURE_WTK'
ELIGIBLE_FLAG_VARIABLE = 'REG_IMP_ELIGIBLE_PV'

STRATA_LEVELS = [
    ['FLOW', 'PURPOSE_PV', 'STAYIMPCTRYLEVEL1_PV'],
    ['FLOW', 'PURPOSE_PV', 'STAYIMPCTRYLEVEL2_PV'],
    ['FLOW', 'PURPOSE_PV', 'STAYIMPCTRYLEVEL3_PV'],
    ['FLOW', 'PURPOSE_PV', 'STAYIMPCTRYLEVEL4_PV']
]

TOWN_CODE_VARIABLE = 'TOWNCODE'
NIGHTS_VARIABLE = 'NIGHTS'
INFO_PRESENT_MARKER = 'INFO_PRESENT_MKR'
FLOW_VARIABLE = 'FLOW'
TOWN_CODE1_VARIABLE = 'TOWNCODE1'
NUMBER_OF_NIGHTS = 9


def ips_correct_regional_nights(row):
    """
    Author       : Thomas Mahoney
    Date         : 12 / 03 / 2018
    Purpose      : Corrects the regional nights data.
    Parameters   : df_input - the IPS survey records for the period.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Adjust regional night figures so that they match overall STAY_VARIABLE
    if row[INFO_PRESENT_MARKER] == 1:
        known_town_nk_nights = 0
        nights_sum = 0

        # Compute nights_sum and known_town_nk_nights for this record
        for x in range(1, NUMBER_OF_NIGHTS):
            if row[TOWN_CODE_VARIABLE + str(x)] != 99999 and not math.isnan(row[TOWN_CODE_VARIABLE + str(x)]):
                if not math.isnan(row[NIGHTS_VARIABLE + str(x)]):
                    nights_sum = nights_sum + row[NIGHTS_VARIABLE + str(x)]
                else:
                    known_town_nk_nights = known_town_nk_nights + 1

        if known_town_nk_nights == 0:
            # Check if sum of nights is not equal to stay
            if nights_sum != row[STAY_VARIABLE]:
                stay_sum = (row[STAY_VARIABLE] / nights_sum)

                for x in range(1, NUMBER_OF_NIGHTS):
                    if row[TOWN_CODE_VARIABLE + str(x)] != 99999 and not math.isnan(
                            row[TOWN_CODE_VARIABLE + str(x)]):
                        row[NIGHTS_VARIABLE + str(x)] = row[NIGHTS_VARIABLE + str(x)] * stay_sum
                        row[STAY_VARIABLE + str(x) + 'K'] = 'K'
        else:
            # If town has known code add STAY_VARIABLE to total nights_sum
            # if town is null adds 1 to unknown
            if nights_sum >= row[STAY_VARIABLE]:
                for x in range(1, NUMBER_OF_NIGHTS):
                    if row[TOWN_CODE_VARIABLE + str(x)] != 99999 and not math.isnan(
                            row[TOWN_CODE_VARIABLE + str(x)]) and math.isnan(row[NIGHTS_VARIABLE + str(x)]):
                        row[NIGHTS_VARIABLE + str(x)] = 1
                        nights_sum = nights_sum + row[NIGHTS_VARIABLE + str(x)]

                # Calculate nights uplift factor
                stay_sum = (row[STAY_VARIABLE] / nights_sum)

                for x in range(1, NUMBER_OF_NIGHTS):
                    if row[TOWN_CODE_VARIABLE + str(x)] != 99999 and not math.isnan(
                            row[TOWN_CODE_VARIABLE + str(x)]):
                        row[NIGHTS_VARIABLE + str(x)] = row[NIGHTS_VARIABLE + str(x)] * stay_sum
                        row[STAY_VARIABLE + str(x) + 'K'] = 'L'

            else:
                for x in range(1, NUMBER_OF_NIGHTS):
                    if row[TOWN_CODE_VARIABLE + str(x)] != 99999 and not math.isnan(
                            row[TOWN_CODE_VARIABLE + str(x)]) and math.isnan(row[NIGHTS_VARIABLE + str(x)]):
                        row[NIGHTS_VARIABLE + str(x)] = (row[STAY_VARIABLE] - nights_sum) / known_town_nk_nights
                        row[STAY_VARIABLE + str(x) + 'K'] = 'M'

    return row


def do_ips_regional_weight_calculation(df_input_data, serial_num, final_weight):
    """
    Author       : Thomas Mahoney
    Date         : 12 / 03 / 2018
    Purpose      : Calculates regional weights for IPS.
    Parameters   : df_input_data - the IPS survey records for the period
                   var_serial - Variable holding the record serial number
                   var_final_weight - the name of the final weight variable
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    night_columns = [NIGHTS_VARIABLE + str(i) for i in range(1, NUMBER_OF_NIGHTS)]
    stay_columns = [STAY_VARIABLE + str(i) + 'K' for i in range(1, NUMBER_OF_NIGHTS)]

    df_input_data[night_columns] = df_input_data[night_columns].fillna(np.NaN)

    df_input_data[stay_columns] = df_input_data[stay_columns].fillna('')

    for x in range(1, NUMBER_OF_NIGHTS):
        df_input_data[TOWN_CODE_VARIABLE + str(x)] = df_input_data[TOWN_CODE_VARIABLE + str(x)].fillna(np.NaN)

    # Extract only eligible rows
    df_impute_towns = df_input_data[df_input_data[ELIGIBLE_FLAG_VARIABLE] == 1]

    # Set initial values for wt and wkt columns
    df_impute_towns.loc[:, STAY_WEIGHT_VARIABLE] = 1
    df_impute_towns.loc[:, STAY_WEIGHTK_VARIABLE] = ''

    df_impute_towns.loc[:, VISIT_WEIGHT_VARIABLE] = 1
    df_impute_towns.loc[:, VISIT_WEIGHTK_VARIABLE] = ''

    df_impute_towns.loc[:, EXPENDITURE_WEIGHT_VARIABLE] = 1
    df_impute_towns.loc[:, EXPENDITURE_WEIGHTK_VARIABLE] = ''

    # Check if towncode information is present for the input data
    def check_info(row):

        if row[TOWN_CODE1_VARIABLE] == 99999 or math.isnan(row[TOWN_CODE1_VARIABLE]):
            row[INFO_PRESENT_MARKER] = 0
        else:
            row[INFO_PRESENT_MARKER] = 1

        return row

    df_impute_towns = df_impute_towns.apply(check_info, axis=1)

    # Correct nights information so that it matches stay
    df_temp1 = df_impute_towns.apply(ips_correct_regional_nights, axis=1)

    # Extract the corrected data and sort
    df_temp1 = df_temp1[[FLOW_VARIABLE, serial_num] + night_columns + stay_columns].sort_values(serial_num)

    df_impute_towns_ext = df_impute_towns.sort_values(serial_num)

    # Update df_impute_towns_ext info with the corrected data.
    df_impute_towns_ext.update(df_temp1)

    # Generate lists to hold the loop data frames
    seg: list = [0] * 4
    df_temp2: list = [0] * 4
    df_temp3: list = [0] * 4
    segment: list = [0] * 4
    cseg: list = [0] * 4
    trunc_segment: list = [0] * 4

    # Loop over imputation levels
    for level in range(1, MAXIMUM_LEVEL + 1):
        strata = STRATA_LEVELS[level - 1]

        df_impute_towns_ext['VISIT_WTK_NONMISS'] = \
            np.where(df_impute_towns_ext[VISIT_WEIGHTK_VARIABLE] != '', 1, np.nan)
        df_impute_towns_ext['VISIT_WTK_ALL'] = 1

        # Sort df_impute_towns_ext by strata
        df_impute_towns_ext = df_impute_towns_ext.sort_values(strata)

        # Replace blank values with -1 as python drops blanks during the aggregation process.
        df_impute_towns_ext[strata] = df_impute_towns_ext[strata].fillna(-1)

        # Calculate the number of records in each segment that have previously
        # been uplifted and the total number of records in each segment
        df_impute_towns_ext1 = df_impute_towns_ext.groupby(strata)['VISIT_WTK_NONMISS'].agg(['count'])
        df_impute_towns_ext1.rename(columns={'count': 'VISIT_WT_COUNT', }, inplace=True)

        df_impute_towns_ext2 = df_impute_towns_ext.groupby(strata)['VISIT_WTK_ALL'].agg(['count'])
        df_impute_towns_ext2.rename(columns={'count': 'TOTAL_COUNT', }, inplace=True)

        # Flatten the column structure after adding the new columns above
        df_impute_towns_ext1 = df_impute_towns_ext1.reset_index()
        df_impute_towns_ext2 = df_impute_towns_ext2.reset_index()

        # Merge the two data sets generated
        seg[level - 1] = pd.merge(df_impute_towns_ext1, df_impute_towns_ext2, on=strata, how='inner')

        # Replace the previously added -1 values with their original blank values
        df_impute_towns_ext[strata] = df_impute_towns_ext[strata].replace(-1, np.NaN)
        seg[level - 1][strata] = seg[level - 1][strata].replace(-1, np.NaN)

        # Copy the data and calculate the visit, stay and expenditure weights
        df_impute_towns_ext_mod = df_impute_towns_ext.copy()

        df_impute_towns_ext_mod['FIN'] = df_impute_towns_ext_mod[final_weight] * df_impute_towns_ext_mod[
            VISIT_WEIGHT_VARIABLE]
        df_impute_towns_ext_mod['STY'] = df_impute_towns_ext_mod[STAY_VARIABLE] * df_impute_towns_ext_mod[
            final_weight] * df_impute_towns_ext_mod[STAY_WEIGHT_VARIABLE]
        df_impute_towns_ext_mod['EXP'] = df_impute_towns_ext_mod[SPEND_VARIABLE] * df_impute_towns_ext_mod[
            final_weight] * df_impute_towns_ext_mod[EXPENDITURE_WEIGHT_VARIABLE]

        # Compute weight totals over good records
        df_temp2[level - 1] = df_impute_towns_ext_mod.loc[df_impute_towns_ext_mod[INFO_PRESENT_MARKER] == 1]

        # Replace blank values with -1 as python drops blanks during the aggregation process.
        df_temp2[level - 1][strata] = df_temp2[level - 1][strata].fillna(-1)

        df_temp2_count = df_temp2[level - 1].groupby(strata)['FIN'].agg(['count'])
        df_temp2_count.rename(columns={'count': 'TOWN_COUNT', }, inplace=True)

        df_temp2_fin = df_temp2[level - 1].groupby(strata)['FIN'].agg(['sum'])
        df_temp2_fin.rename(columns={'sum': 'KNOWN_FINAL_WEIGHTS', }, inplace=True)

        df_temp2_sty = df_temp2[level - 1].groupby(strata)['STY'].agg(['sum'])
        df_temp2_sty.rename(columns={'sum': 'KNOWN_STAY', }, inplace=True)

        df_temp2_exp = df_temp2[level - 1].groupby(strata)['EXP'].agg(['sum'])
        df_temp2_exp.rename(columns={'sum': 'KNOWN_EXPEND', }, inplace=True)

        # Flatten the column structure after generating the new columns above
        df_temp2_count = df_temp2_count.reset_index()
        df_temp2_fin = df_temp2_fin.reset_index()
        df_temp2_sty = df_temp2_sty.reset_index()
        df_temp2_exp = df_temp2_exp.reset_index()

        # Merge the generated values into one data frame
        df_temp2[level - 1] = pd.merge(df_temp2_count, df_temp2_fin, on=strata, how='inner')
        df_temp2[level - 1] = pd.merge(df_temp2[level - 1], df_temp2_sty, on=strata, how='inner')
        df_temp2[level - 1] = pd.merge(df_temp2[level - 1], df_temp2_exp, on=strata, how='inner')

        # Replace the previously added -1 values with their original blank values
        df_temp2[level - 1][strata] = df_temp2[level - 1][strata].replace(-1, np.NaN)

        # Compute weight totals over bad records
        df_temp3[level - 1] = df_impute_towns_ext_mod[df_impute_towns_ext_mod[INFO_PRESENT_MARKER] == 0]

        # Replace blank values with -1 as python drops blanks during the aggregation process.
        df_temp3[level - 1][strata] = df_temp3[level - 1][strata].fillna(-1)

        df_temp3_count = df_temp3[level - 1].groupby(strata)['FIN'].agg(['count'])
        df_temp3_count.rename(columns={'count': 'NO_TOWN_COUNT', }, inplace=True)

        df_temp3_fin = df_temp3[level - 1].groupby(strata)['FIN'].agg(['sum'])
        df_temp3_fin.rename(columns={'sum': 'UNKNOWN_FINAL_WEIGHT', }, inplace=True)

        df_temp3_sty = df_temp3[level - 1].groupby(strata)['STY'].agg(['sum'])
        df_temp3_sty.rename(columns={'sum': 'UNKNOWN_STAY', }, inplace=True)

        df_temp3_exp = df_temp3[level - 1].groupby(strata)['EXP'].agg(['sum'])
        df_temp3_exp.rename(columns={'sum': 'UNKNOWN_EXPEND', }, inplace=True)

        # Flatten the column structure after generating the new columns above
        df_temp3_count = df_temp3_count.reset_index()
        df_temp3_fin = df_temp3_fin.reset_index()
        df_temp3_sty = df_temp3_sty.reset_index()
        df_temp3_exp = df_temp3_exp.reset_index()

        # Merge the generated values into one data frame
        df_temp3[level - 1] = pd.merge(df_temp3_count, df_temp3_fin, on=strata, how='inner')
        df_temp3[level - 1] = pd.merge(df_temp3[level - 1], df_temp3_sty, on=strata, how='inner')
        df_temp3[level - 1] = pd.merge(df_temp3[level - 1], df_temp3_exp, on=strata, how='inner')

        # Replace the previously added -1 values with their original blank values
        df_temp3[level - 1][strata] = df_temp3[level - 1][strata].replace(-1, np.NaN)

        # Sort the generated data frames by strata before merging them together
        seg[level - 1] = seg[level - 1].sort_values(strata)
        df_temp2[level - 1] = df_temp2[level - 1].sort_values(strata)
        df_temp3[level - 1] = df_temp3[level - 1].sort_values(strata)

        # Merge good and bad totals into the data
        segment[level - 1] = pd.merge(seg[level - 1], df_temp2[level - 1], on=strata, how='left')
        segment[level - 1] = pd.merge(segment[level - 1], df_temp3[level - 1], on=strata, how='left')

        # Account for missing values by setting weights to zero
        segment[level - 1].loc[segment[level - 1]['UNKNOWN_FINAL_WEIGHT'].isnull(), 'UNKNOWN_FINAL_WEIGHT'] = 0
        segment[level - 1].loc[segment[level - 1]['UNKNOWN_STAY'].isnull(), 'UNKNOWN_STAY'] = 0
        segment[level - 1].loc[segment[level - 1]['UNKNOWN_EXPEND'].isnull(), 'UNKNOWN_EXPEND'] = 0
        segment[level - 1].loc[segment[level - 1]['TOTAL_COUNT'].isnull(), 'TOTAL_COUNT'] = 0
        segment[level - 1].loc[segment[level - 1]['TOWN_COUNT'].isnull(), 'TOWN_COUNT'] = 0
        segment[level - 1].loc[segment[level - 1]['NO_TOWN_COUNT'].isnull(), 'NO_TOWN_COUNT'] = 0

        # Replace blank values with -1 and sort the data by strata
        segment[level - 1][strata] = segment[level - 1][strata].fillna(-1)
        segment[level - 1] = segment[level - 1].sort_values(strata)

        # Replace the previously added -1 values with their original blank values
        segment[level - 1][strata] = segment[level - 1][strata].replace(-1, np.NaN)

        # Look for records that still need to be uplifted
        cseg[level - 1] = segment[level - 1].loc[
            segment[level - 1]['TOWN_COUNT'] != segment[level - 1]['VISIT_WT_COUNT']]

        # Count the number of records found that still need uplifting
        record_count = len(cseg[level - 1].index)

        if record_count > 0:
            # Remove invalid groups from the imputation set

            # Check current level, as level 4 thresholds are different to 1-3
            if level < 4:
                trunc_segment[level - 1] = segment[level - 1].loc[
                    (segment[level - 1]['VISIT_WT_COUNT'] < segment[level - 1]['TOTAL_COUNT'])]

                condition = (
                        (trunc_segment[level - 1]['TOWN_COUNT'] >= 20)
                        & (trunc_segment[level - 1]['NO_TOWN_COUNT'] < trunc_segment[level - 1]['TOWN_COUNT'])
                        & (trunc_segment[level - 1]['KNOWN_EXPEND'] != 0)
                        & (trunc_segment[level - 1]['KNOWN_STAY'] != 0)
                        & (trunc_segment[level - 1]['KNOWN_FINAL_WEIGHTS'] != 0)
                        & (trunc_segment[level - 1]['KNOWN_FINAL_WEIGHTS'].notnull())
                        & (((trunc_segment[level - 1]['KNOWN_FINAL_WEIGHTS']
                             + trunc_segment[level - 1]['UNKNOWN_FINAL_WEIGHT'])
                            / trunc_segment[level - 1]['KNOWN_FINAL_WEIGHTS']) <= 2)
                        & (((trunc_segment[level - 1]['KNOWN_STAY'] + trunc_segment[level - 1]['UNKNOWN_STAY'])
                            / trunc_segment[level - 1]['KNOWN_STAY']) <= 2)
                        & (((trunc_segment[level - 1]['KNOWN_EXPEND'] + trunc_segment[level - 1]['UNKNOWN_EXPEND'])
                            / trunc_segment[level - 1]['KNOWN_EXPEND']) <= 2)
                )

                trunc_segment[level - 1] = trunc_segment[level - 1].loc[condition]

            if level > 3:
                trunc_segment[level - 1] = segment[level - 1].loc[
                    (segment[level - 1]['VISIT_WT_COUNT'] < segment[level - 1]['TOTAL_COUNT'])]

                condition = ((trunc_segment[level - 1]['KNOWN_EXPEND'] != 0)
                             & (trunc_segment[level - 1]['KNOWN_STAY'] != 0)
                             & (trunc_segment[level - 1]['KNOWN_FINAL_WEIGHTS'] != 0))

                trunc_segment[level - 1] = trunc_segment[level - 1].loc[condition]

            # Sort trunc_segment before merging into the df_impute_towns_ext data frame
            trunc_segment[level - 1] = trunc_segment[level - 1].sort_values(strata)

            # Select data to be merged
            trunc_segment[level - 1] = trunc_segment[level - 1][strata + ['VISIT_WT_COUNT', 'TOTAL_COUNT',
                                                                          'KNOWN_FINAL_WEIGHTS', 'KNOWN_STAY',
                                                                          'KNOWN_EXPEND', 'UNKNOWN_FINAL_WEIGHT',
                                                                          'UNKNOWN_STAY', 'UNKNOWN_EXPEND']]

            # Sort df_impute_towns_ext before merge
            df_impute_towns_ext = df_impute_towns_ext.sort_values(strata)

            # Join the known and unknown weights onto the original data
            df_impute_towns_ext = pd.merge(df_impute_towns_ext, trunc_segment[level - 1], on=strata, how='left')

            # Calculate the revised weights
            def calculate_revised_weights(row):

                if row['KNOWN_FINAL_WEIGHTS'] != 0 and not math.isnan(row['KNOWN_FINAL_WEIGHTS']):

                    if row[VISIT_WEIGHTK_VARIABLE] == '':
                        row[VISIT_WEIGHTK_VARIABLE] = str(level)
                        row[STAY_WEIGHTK_VARIABLE] = str(level)
                        row[EXPENDITURE_WEIGHTK_VARIABLE] = str(level)

                    if row[INFO_PRESENT_MARKER] == 1 and row[VISIT_WEIGHTK_VARIABLE] == str(level):
                        row[VISIT_WEIGHT_VARIABLE] = row[VISIT_WEIGHT_VARIABLE] * (
                                row['KNOWN_FINAL_WEIGHTS'] + row['UNKNOWN_FINAL_WEIGHT']) / row[
                                                         'KNOWN_FINAL_WEIGHTS']
                        row[STAY_WEIGHT_VARIABLE] = row[STAY_WEIGHT_VARIABLE] * (
                                row['KNOWN_STAY'] + row['UNKNOWN_STAY']) / row['KNOWN_STAY']
                        row[EXPENDITURE_WEIGHT_VARIABLE] = row[EXPENDITURE_WEIGHT_VARIABLE] * (
                                row['KNOWN_EXPEND'] + row['UNKNOWN_EXPEND']) / row['KNOWN_EXPEND']

                elif row[INFO_PRESENT_MARKER] == 0:

                    row[VISIT_WEIGHT_VARIABLE] = 0
                    row[STAY_WEIGHT_VARIABLE] = 0
                    row[EXPENDITURE_WEIGHT_VARIABLE] = 0
                    pass

                return row

            df_impute_towns_ext = df_impute_towns_ext.apply(calculate_revised_weights, axis=1)

            # Drop the no longer needed columns
            df_impute_towns_ext = df_impute_towns_ext.drop(columns=['KNOWN_FINAL_WEIGHTS', 'KNOWN_STAY',
                                                                    'KNOWN_EXPEND', 'UNKNOWN_FINAL_WEIGHT',
                                                                    'UNKNOWN_STAY', 'UNKNOWN_EXPEND'])

        # if record_count > 0

    # Loop end

    # Extract the required data from the looped dataset
    df_output_data = df_impute_towns_ext[[serial_num,
                                          VISIT_WEIGHT_VARIABLE, STAY_WEIGHT_VARIABLE, EXPENDITURE_WEIGHT_VARIABLE,
                                          VISIT_WEIGHTK_VARIABLE, STAY_WEIGHTK_VARIABLE, EXPENDITURE_WEIGHTK_VARIABLE] +
                                         night_columns + stay_columns]

    def round_wts(row):
        row['VISIT_WTK'] = pd.to_numeric(row['VISIT_WTK'], errors="coerce")
        row['STAY_WTK'] = pd.to_numeric(row['STAY_WTK'], errors="coerce")
        row['EXPENDITURE_WTK'] = pd.to_numeric(row['EXPENDITURE_WTK'], errors="coerce")

        row['VISIT_WTK'] = ips_rounding(row['VISIT_WTK'], 1)
        row['STAY_WTK'] = ips_rounding(row['STAY_WTK'], 1)
        row['EXPENDITURE_WTK'] = ips_rounding(row['EXPENDITURE_WTK'], 1)

        row[VISIT_WEIGHT_VARIABLE] = ips_rounding(row[VISIT_WEIGHT_VARIABLE], 3)
        row[STAY_WEIGHT_VARIABLE] = ips_rounding(row[STAY_WEIGHT_VARIABLE], 3)
        row[EXPENDITURE_WEIGHT_VARIABLE] = ips_rounding(row[EXPENDITURE_WEIGHT_VARIABLE], 3)

        row[stay_columns] = pd.to_numeric(row[stay_columns], errors="ignore")

        return row

    df_output_data = df_output_data.apply(round_wts, axis=1)

    df_output_data[stay_columns].fillna(np.nan, inplace=True)
    df_output_data['STAY_WTK'].fillna(np.nan, inplace=True)
    df_output_data['EXPENDITURE_WTK'].fillna(np.nan, inplace=True)
    df_output_data['VISIT_WTK'].fillna(np.nan, inplace=True)

    # Sort the output data frame
    df_output_data = df_output_data.sort_values(serial_num)

    return df_output_data
