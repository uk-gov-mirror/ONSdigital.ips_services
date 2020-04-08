import math
import numpy as np
import pandas as pd
from ips.util.services_logging import log
from ips.services.calculations import log_warnings
from ips.services.calculations.sas_rounding import ips_rounding

OUTPUT_TABLE_NAME = 'SAS_RAIL_IMP'
ELIGIBLE_VARIABLE = 'FLOW'  # direction of travel (use 5 out uk , 8 in )
COUNT_VARIABLE = 'COUNT'
STRATA = ['FLOW', 'RAIL_CNTRY_GRP_PV']
RAIl_FARE_VARIABLE = 'RAIL_EXERCISE_PV'
SPEND_VARIABLE = 'SPEND'
PRESPEND_VARIABLE = 'PRESPEND'
GROSS_PRESPEND_VARIABLE = 'GROSSPRESPEND'
RAIL_EXPENSE_VARIABLE = 'RAILEXP'
RAIL_FACTOR_VARIABLE = 'RAIL_FACTOR'


def do_ips_railex_imp(df_input, var_serial, var_final_weight, minimum_count_threshold, run_id=None):
    """
    Author       : Thomas Mahoney
    Date         : 28 / 02 / 2018
    Purpose      : Calculates the imputed values for rail expenditure for the IPS system.
    Parameters   : df_input - the IPS survey dataset
                   output - the output dataset
                   var_serial - the serial number field name
                   var_final_weight - previously estimated final weight
                   minimum_count_threshold - threshold for respondent count warning msg
    Returns      : df_output(dataframe containing serial number and calculated spend value)
    Requirements : NA
    Dependencies : NA
    """

    # Sort the df_input data by flow and rail country
    df_input = df_input.sort_values(STRATA)

    # Create second data set containing records where flow is not null
    input2 = df_input[np.isfinite(df_input[ELIGIBLE_VARIABLE])]

    # Calculate the PRESPEND_VARIABLE column value using the SPEND_VARIABLE and var_final_weight column values.
    input2[PRESPEND_VARIABLE] = input2[SPEND_VARIABLE] * input2[var_final_weight]

    input2 = input2.sort_values(STRATA)

    # Replace blank values with zero as python drops blanks during the aggregation process.
    input2[STRATA] = input2[STRATA].fillna(0)

    # Generate the aggregated data
    gp_summin = input2.groupby(STRATA)[PRESPEND_VARIABLE].agg(['sum', 'count'])
    gp_summin.rename(columns={'sum': GROSS_PRESPEND_VARIABLE, 'count': COUNT_VARIABLE}, inplace=True)

    railexp_summin = input2.groupby(STRATA)[RAIl_FARE_VARIABLE].agg(['mean'])
    railexp_summin.rename(columns={'mean': RAIL_EXPENSE_VARIABLE}, inplace=True)

    # Reset the data frames index to include the new columns generated
    gp_summin = gp_summin.reset_index()
    railexp_summin = railexp_summin.reset_index()

    # Merge the generated data sets into one
    df_summin = pd.merge(gp_summin, railexp_summin, how='inner')

    # Replace the previously filled blanks with their original values
    df_summin[STRATA] = df_summin[STRATA].replace(0, np.NaN)

    # Report any cells with respondent counts below the minCountThreshold

    # Create data set for rows below the threshold
    df_summin_thresholds_check = df_summin[
        (df_summin[COUNT_VARIABLE] < minimum_count_threshold)
    ]

    # Output the values below of the threshold to the logger
    if len(df_summin_thresholds_check) > 0:
        log_warnings("Minimums weight outside thresholds for")(df_summin_thresholds_check, 2, run_id, 11)

    # Calculate each row's rail factor
    def calculate_rail_factor(row):
        if row[GROSS_PRESPEND_VARIABLE] == 0:
            row[RAIL_FACTOR_VARIABLE] = np.NaN
        else:
            row[RAIL_FACTOR_VARIABLE] = (row[GROSS_PRESPEND_VARIABLE]
                                         + row[RAIL_EXPENSE_VARIABLE]) / row[GROSS_PRESPEND_VARIABLE]
        return row

    df_summinsum = df_summin.apply(calculate_rail_factor, axis=1)

    # Sort the calculated data frame by the STRATA ready to be merged
    df_summinsum = df_summinsum.sort_values(STRATA)

    # Append the calculated values to the input data set (generating our output)
    df_output = pd.merge(df_input, df_summinsum, on=STRATA, how='left')

    # Calculate the spend of the output data set
    def calculate_spend(row):
        if not math.isnan(row[RAIL_FACTOR_VARIABLE]):
            if not math.isnan(row[SPEND_VARIABLE]):
                row[SPEND_VARIABLE] = ips_rounding(row[SPEND_VARIABLE] * row[RAIL_FACTOR_VARIABLE], 0)
        return row

    df_output = df_output.apply(calculate_spend, axis=1)

    # Keep only the 'SERIAL' and 'SPEND' columns
    df_output = df_output[[var_serial, SPEND_VARIABLE]]

    # Return the generated data frame to be appended to oracle
    return df_output
