import numpy as np
from ips.util.services_logging import log


def ips_impute(df_input, var_serial_num, strata_base_list, thresh_base_list, num_levels,
               impute_var, var_value, impute_function, var_impute_flag, var_impute_level):
    # Create the donor set, in which the impute flag is false
    df_output = df_input

    df_to_impute = df_input.loc[df_input[var_impute_flag] == 1.0]

    # Create recipient set, in which the impute flag is true
    df_impute_from = df_input.loc[df_input[var_impute_flag] == 0.0]

    level: int = 0

    count = 'COUNT'

    df_output[count] = 0

    # Loop until no more records can be imputed or max  number of iterations is reached
    while (level < num_levels) & (not df_to_impute.empty):
        # key_name = 'df_output_match_' + str(level)

        # strata_base_list is a list containing other lists, which control sorting
        # at each iteration level.
        # These lists need to be hard coded and passed in from the calling procedure.
        # Only the list for the current iteration is passed from strata_base_list

        # Calculate the imputed values to be used
        df_segment_output = ips_impute_segment(df_impute_from,
                                               level,
                                               strata_base_list[level],
                                               impute_var,
                                               impute_function,
                                               var_value,
                                               count,
                                               thresh_base_list[level])

        # Use values from segment output to impute missing values 
        df_output_frames = ips_impute_match(df_to_impute,
                                            df_segment_output,
                                            df_output,
                                            strata_base_list[level],
                                            var_value,
                                            level,
                                            var_impute_level,
                                            var_impute_flag,
                                            count)

        # Store output from each iteration, which is fed into next iteration
        df_output = df_output_frames[0]

        # Stores the remaining values needing imputing
        df_to_impute = df_output_frames[1]

        level += 1

    # Tidy up the output by keeping only non-missing data and a subset of columns
    columns_to_keep = [var_serial_num, var_value, var_impute_level]
    df_output = df_output[columns_to_keep]

    df_output = df_output.dropna()

    return df_output


def ips_impute_segment(df_input, level, strata, impute_var, function, var_value,
                       var_count, thresh):
    df_input = df_input.sort_values(strata)

    # Ensure rows with missing data aren't excluded indiscriminately
    df_input[strata] = df_input[strata].fillna(-1)

    # Impute values from donor variable
    df_output = (df_input.groupby(strata)[impute_var]
                 # first perform aggregation
                 .agg([str(function), 'count'])
                 # rename output columns to something more meaningful
                 .rename(columns={str(function): var_value + str(level), 'count': 'COUNT' + str(level)})
                 # Flatten column structure back into one index
                 .reset_index())

    # Change dataset missing values back to missing instead of -1
    df_output = df_output.replace(-1, np.NaN)

    # Keep only rows where count is above threshold. New count column created for
    # each iteration to allow easier aggregation of count data later on
    df_output = df_output.where(df_output[var_count + str(level)] > thresh)

    # Remove rows still containing missing data
    df_output = df_output.dropna(thresh=2)

    return df_output


def ips_impute_match(remainder, df_input, output, strata, var_value, level,
                     var_level, var_impute_flag, var_count):
    # Create sorted dataframes from passed-in data

    df_remainder = remainder.sort_values(strata)

    df_input = df_input.sort_values(strata)

    # Merge all data and indicate where the data is found. Keep only rows that are found in df_remainder
    df_remainder.merge(df_input, how="left")

    # df_remainder = df_remainder.drop('_merge', axis = 1)
    df_remainder = df_remainder.reset_index(drop=True)

    df_remainder.sort_values(strata, inplace=True)

    # Merge current output data with donor dataframe
    # Indicator = True creates a new column '_merge' which identifies which
    # dataset contributed each column. This column is used further below.
    df_output = output

    df_output = df_output.merge(df_input, how="left", on=strata, indicator=True)

    df_output.sort_values(strata, inplace=True)

    # Setup iteration specific column name variables
    value_level = str(var_value) + str(level)
    count_level = str(var_count) + str(level)

    # Update output with imputed values
    # If conditions are met, set value column to be imputed to value of the 
    # calculate imputed column, collect the count value and record which 
    # iteration level this row was imputed in.

    if var_value not in df_output.columns:
        df_output[var_value] = 0

    condition = (df_output[var_level].isnull()) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] == 'both')

    df_output[var_value] = np.where(condition, df_output[value_level], df_output[var_value])
    df_output[var_count] = np.where(condition, df_output[count_level], df_output[var_count])
    df_output[var_level] = np.where(condition, level, df_output[var_level])

    # Remove merge origin tracking column from output
    df_output = df_output.drop('_merge', axis=1)
    df_output = df_output.reset_index(drop=True)

    df_output = df_output.sort_values(strata)

    return df_output, df_remainder
