from ips.util.services_logging import log
import numpy as np
import pandas as pd

from ips.services.calculations import ips_impute as imp

OUTPUT_TABLE_NAME = "SAS_SPEND_IMP"
STEM_VARIABLE = [
    ["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR1_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR2_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR1_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR2_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR1_PV", "PUR2_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR2_PV", "PUR2_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR2_PV", "PUR2_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR2_PV"],
    ["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR3_PV"],
    ["UK_OS_PV", "DUR2_PV", "PUR3_PV"]
]
STEM_THRESHOLD = [19, 12, 12, 12, 12, 12, 12, 12, 0, 0]
DONOR_VARIABLE = "SPEND"
OTHER_DONOR_VARIABLE = "XPD"
OUTPUT_VARIABLE = "NEWSPEND"
ELIGIBLE_FLAG_VARIABLE = "SPEND_IMP_ELIGIBLE_PV"
IMPUTATION_FLAG_VARIABLE = "SPEND_IMP_FLAG_PV"
IMPUTATION_LEVEL_VARIABLE = "SPENDK"
STAY_VARIABLE = "STAY"
STAYDAYS_VARIABLE = "STAYDAYS"


def do_ips_spend_imputation(df_survey_data, var_serial, measure):
    # TODO: --->
    df_survey_data.to_csv('/Users/ThornE1/PycharmProjects/ips_services/tests/els scratch folder/spend_calc_in.csv')

    num_levels = len(STEM_THRESHOLD)

    # Select only the eligible donors and recipients
    df_eligible = df_survey_data.copy()
    df_eligible[STAYDAYS_VARIABLE] = np.where(df_eligible[ELIGIBLE_FLAG_VARIABLE] == 1.0,
                                              (df_eligible[STAY_VARIABLE] + 1.0), np.NaN)
    df_eligible.drop(df_eligible[df_eligible[ELIGIBLE_FLAG_VARIABLE] != 1.0].index,
                     inplace=True)

    def selection(row):
        if row[IMPUTATION_FLAG_VARIABLE] != 1.0:
            if (row[DONOR_VARIABLE] > 0) & (row[STAYDAYS_VARIABLE] > 0):
                row[OTHER_DONOR_VARIABLE] = row[DONOR_VARIABLE] / row[STAYDAYS_VARIABLE]
            elif row[DONOR_VARIABLE] == 0:
                row[OTHER_DONOR_VARIABLE] = 0
            else:
                row[IMPUTATION_FLAG_VARIABLE] = 1
        return row

    df_eligible = df_eligible.apply(selection, axis=1)

    # TODO: --->
    df_eligible.to_csv('/Users/ThornE1/PycharmProjects/ips_services/tests/els scratch folder/before_impute.csv')

    # Perform the imputation
    df_output = imp.ips_impute(df_eligible, var_serial,
                               STEM_VARIABLE, STEM_THRESHOLD, num_levels, OTHER_DONOR_VARIABLE,
                               OUTPUT_VARIABLE, measure, IMPUTATION_FLAG_VARIABLE, IMPUTATION_LEVEL_VARIABLE)

    # Merge and cleanse
    df_final_output = pd.merge(df_eligible, df_output, on=var_serial, how='left')
    df_final_output.drop(IMPUTATION_LEVEL_VARIABLE + "_x", axis=1, inplace=True)
    df_final_output.rename(columns={IMPUTATION_LEVEL_VARIABLE + "_y": IMPUTATION_LEVEL_VARIABLE}, inplace=True)

    # TODO: --->
    df_output.to_csv('/Users/ThornE1/PycharmProjects/ips_services/tests/els scratch folder/after_impute.csv')

    # Create final output with required columns
    df_final_output = df_final_output[[var_serial, OUTPUT_VARIABLE, IMPUTATION_LEVEL_VARIABLE,
                                       STAYDAYS_VARIABLE]]
    df_final_output.loc[df_final_output[IMPUTATION_LEVEL_VARIABLE].notnull(),
                        OUTPUT_VARIABLE] = (df_final_output[OUTPUT_VARIABLE] * df_final_output[STAYDAYS_VARIABLE])
    df_final_output[OUTPUT_VARIABLE] = df_final_output[OUTPUT_VARIABLE].apply(lambda x: round(x, 0))

    # Cleanse df before returning
    df_final_output = df_final_output[[var_serial, IMPUTATION_LEVEL_VARIABLE, OUTPUT_VARIABLE]]

    # TODO: --->
    df_final_output.to_csv('/Users/ThornE1/PycharmProjects/ips_services/tests/els scratch folder/spend_calc_final.csv')

    return df_final_output
