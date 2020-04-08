import pandas as pd
import numpy as np

from ips.util.services_logging import log

OUTPUT_TABLE_NAME = "SAS_IMBALANCE_WT"
SUMMARY_TABLE_NAME = "SAS_PS_IMBALANCE"
PORTROUTE_COLUMN = "PORTROUTE"
FLOW_COLUMN = "FLOW"
DIRECTION_COLUMN = "ARRIVEDEPART"
PG_FACTOR_COLUMN = "IMBAL_PORT_FACT_PV"
CG_FACTOR_COLUMN = "IMBAL_CTRY_FACT_PV"
PRIOR_SUM_COLUMN = "SUM_PRIOR_WT"
POST_SUM_COLUMN = "SUM_IMBAL_WT"
ELIGIBLE_FLAG_COLUMN = "IMBAL_ELIGIBLE_PV"


def do_ips_imbweight_calculation(df_survey_data, serial, shift_weight, non_response_weight, min_weight,
                                 traffic_weight, oo_weight, imbalance_weight):
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       : Calculates imbalance weight    
    Parameters    : CURRENTLY - df_survey_data = "SAS_SURVEY_SUBSAMPLE"
                               , var_serialNum = "SERIAL"
                               , var_shiftWeight = "SHIFT_WT"
                               , var_NRWeight = "NON_RESPONSE_WT"
                               , var_minWeight = "MINS_WT"
                               , var_trafficWeight = "TRAFFIC_WT"
                               , var_OOHWeight = "UNSAMP_TRAFFIC_WT"
                               , var_imbalanceWeight = "IMBAL_WT"
    Returns       : Output and Summary dataframes  
    """

    # Do some initial setup and selection
    df_output_data = df_survey_data.copy()
    df_survey_data.drop(df_output_data[df_output_data[ELIGIBLE_FLAG_COLUMN] == 1.0].index, inplace=True)
    df_output_data.drop(df_output_data[df_output_data[ELIGIBLE_FLAG_COLUMN] != 1.0].index, inplace=True)
    df_output_data.loc[df_output_data[ELIGIBLE_FLAG_COLUMN] == 1.0, imbalance_weight] = 1.0

    # Create total traffic dataframe
    df_total_traffic = df_output_data[[PORTROUTE_COLUMN, FLOW_COLUMN]].copy()
    df_total_traffic.sort_values([PORTROUTE_COLUMN, FLOW_COLUMN])

    df_total_traffic["TOT_NI_TRAFFIC"] = (df_output_data[shift_weight]
                                          * df_output_data[non_response_weight]
                                          * df_output_data[min_weight]
                                          * df_output_data[traffic_weight]
                                          * df_output_data[oo_weight])

    df_total_traffic = df_total_traffic.groupby([PORTROUTE_COLUMN, FLOW_COLUMN]).agg({"TOT_NI_TRAFFIC": 'sum'})
    df_total_traffic.reset_index(inplace=True)

    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_output_data[FLOW_COLUMN] == 1) | (df_output_data[FLOW_COLUMN] == 5)
    arrivedepart_condition = df_output_data[DIRECTION_COLUMN] == 2
    df_output_data.loc[flow_condition & arrivedepart_condition,
                       imbalance_weight] = df_output_data[PG_FACTOR_COLUMN]

    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (df_output_data[FLOW_COLUMN] == 3) | (df_output_data[FLOW_COLUMN] == 7)
    arrivedepart_condition = df_output_data[DIRECTION_COLUMN] == 1
    df_output_data.loc[flow_condition & arrivedepart_condition,
                       imbalance_weight] = df_output_data[PG_FACTOR_COLUMN]

    # Update overseas departures with country imbalance
    flow_condition = (df_output_data[FLOW_COLUMN] == 1) | (df_output_data[FLOW_COLUMN] == 5)
    df_output_data.loc[flow_condition, imbalance_weight] = (df_output_data[imbalance_weight]
                                                            * df_output_data[CG_FACTOR_COLUMN])

    # Calculate the pre and post sums for overseas residents
    df_prepost = df_output_data.copy()
    prepost_flow_range = [1, 3, 5, 7]
    df_prepost = df_prepost[df_prepost[FLOW_COLUMN].isin(prepost_flow_range)]
    df_prepost["PRE_IMB_WEIGHTS"] = (df_prepost[shift_weight]
                                     * df_prepost[non_response_weight]
                                     * df_prepost[min_weight]
                                     * df_prepost[traffic_weight]
                                     * df_prepost[oo_weight])
    df_prepost["POST_IMB_WEIGHTS"] = (df_prepost[imbalance_weight]
                                      * df_prepost[shift_weight]
                                      * df_prepost[non_response_weight]
                                      * df_prepost[min_weight]
                                      * df_prepost[traffic_weight]
                                      * df_prepost[oo_weight])

    # Summarise. Group by PORTROUTE & FLOW, & total the pre & post imbalanace weights
    df_prepost.sort_values([PORTROUTE_COLUMN, FLOW_COLUMN])
    df_overseas_residents = df_prepost.groupby([PORTROUTE_COLUMN, FLOW_COLUMN]).agg({
        'PRE_IMB_WEIGHTS': 'sum', 'POST_IMB_WEIGHTS': 'sum'})
    df_overseas_residents = df_overseas_residents.reset_index()
    df_overseas_residents = df_overseas_residents[[PORTROUTE_COLUMN,
                                                   FLOW_COLUMN,
                                                   "PRE_IMB_WEIGHTS",
                                                   "POST_IMB_WEIGHTS"]]

    # Calculate the difference between pre & post imbalance weighting for departures
    # & calculate the ratio of the difference for departures at each port.
    df_calc_departures = df_overseas_residents.copy()
    df_calc_departures[FLOW_COLUMN + 'Extra'] = df_calc_departures[FLOW_COLUMN] + 1
    df_calc_departures = df_calc_departures.merge(df_total_traffic,
                                                  left_on=[PORTROUTE_COLUMN, FLOW_COLUMN + 'Extra'],
                                                  right_on=[PORTROUTE_COLUMN, FLOW_COLUMN])

    # Calculate
    df_calc_departures["DIFFERENCE"] = (df_calc_departures["POST_IMB_WEIGHTS"]
                                        - df_calc_departures["PRE_IMB_WEIGHTS"])
    df_calc_departures["RATIO"] = (df_calc_departures["DIFFERENCE"]
                                   / df_calc_departures["TOT_NI_TRAFFIC"])

    # Cleanse
    df_calc_departures.drop(["PRE_IMB_WEIGHTS", "POST_IMB_WEIGHTS", FLOW_COLUMN + "_y",
                             "TOT_NI_TRAFFIC", FLOW_COLUMN + 'Extra'],
                            axis=1, inplace=True)
    df_calc_departures.rename(columns={FLOW_COLUMN + "_x": FLOW_COLUMN}, inplace=True)

    # Calculate the imbalance weight
    # First, find ratio
    new_val = df_output_data[[serial, PORTROUTE_COLUMN, FLOW_COLUMN]].copy()
    new_val[FLOW_COLUMN + 'Extra'] = new_val[FLOW_COLUMN] - 1
    new_val = new_val.merge(df_calc_departures,
                            left_on=[PORTROUTE_COLUMN, FLOW_COLUMN + 'Extra'],
                            right_on=[PORTROUTE_COLUMN, FLOW_COLUMN])

    # Append Ratio to df and cleanse
    df_output_data = df_output_data.merge(new_val, left_on=serial,
                                          right_on=serial, how='left')
    df_output_data.loc[df_output_data["RATIO"].notnull(),
                       imbalance_weight] = (1.0 - df_output_data["RATIO"])
    df_output_data.rename(columns={PORTROUTE_COLUMN + "_x": PORTROUTE_COLUMN},
                          inplace=True)
    df_output_data.drop([PORTROUTE_COLUMN + "_y"], axis=1, inplace=True)

    # Append the imbalance weight to the input and cleanse
    df_survey_data_concat = pd.concat([df_survey_data, df_output_data], ignore_index=True, sort=True)
    df_survey_data = df_survey_data_concat.reindex(df_survey_data.columns, axis=1)
    df_survey_data.loc[df_survey_data[imbalance_weight].isnull(), imbalance_weight] = 1

    # Create the summary output
    df_survey_data[PRIOR_SUM_COLUMN] = pd.Series(
        df_survey_data[shift_weight]
        * df_survey_data[non_response_weight]
        * df_survey_data[min_weight]
        * df_survey_data[traffic_weight]
        * df_survey_data[oo_weight]
    )

    df_survey_data[POST_SUM_COLUMN] = pd.Series(
        df_survey_data[imbalance_weight]
        * df_survey_data[shift_weight]
        * df_survey_data[non_response_weight]
        * df_survey_data[min_weight]
        * df_survey_data[traffic_weight]
        * df_survey_data[oo_weight]
    )

    df_sliced = df_survey_data[df_survey_data[POST_SUM_COLUMN] > 0]
    df_sliced[imbalance_weight] = df_sliced[imbalance_weight].astype('float')

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.
    df_sliced[FLOW_COLUMN].fillna('NOTHING', inplace=True)

    df_summary_data = df_sliced.groupby([FLOW_COLUMN]).agg({
        PRIOR_SUM_COLUMN: 'sum', POST_SUM_COLUMN: 'sum'})
    df_summary_data = df_summary_data.reset_index()

    # Replace the previously added 'NOTHING' values with their original blank values
    df_summary_data = df_summary_data.replace('NOTHING', np.NaN)

    # Cleanse dataframes before returning
    df_survey_data = df_output_data[['SERIAL', 'IMBAL_WT']].copy()
    df_survey_data.sort_values([serial], inplace=True)

    return df_survey_data, df_summary_data
