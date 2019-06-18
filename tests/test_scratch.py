import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

def test_after():
    sas_after_impute = '/Users/paul/Downloads/after impute.csv'
    python_after_impute = "/Users/paul/Desktop/after_calc.csv"

    sas_df = pd.read_csv(sas_after_impute)
    python_df = pd.read_csv(python_after_impute)

    sas_df = sas_df.sort_values('SERIAL')
    python_df = python_df.sort_values('SERIAL')

    python_df.reset_index(drop=True, inplace=True)
    sas_df.reset_index(drop=True, inplace=True)

    python_df.drop(python_df.columns[0], inplace=True, axis=1)

    assert_frame_equal(sas_df, python_df)

def test_before():
    pauls_csv = '/Users/paul/Downloads/surveydata_dec2017.csv'
    comp = "/Users/paul/Desktop/before_calc.csv"

    pauls_df = pd.read_csv(pauls_csv)
    comp_df = pd.read_csv(comp)

    pauls_df = pauls_df.loc[pauls_df['FARES_IMP_ELIGIBLE_PV'] == 1.0]

    df_ps = pauls_df[
        [
            'INTDATE', 'TYPE_PV',
            'UKPORT1_PV', 'OSPORT1_PV',
            'UKPORT2_PV', 'OSPORT2_PV',
            'UKPORT3_PV', 'OSPORT3_PV',
            'UKPORT4_PV', 'OSPORT4_PV',
            'DVFARE', 'FARE',
            'FARES_IMP_FLAG_PV',
            'FAREK'
        ]
    ]

    df_ps['FARE'] = np.nan
    df_ps['FAREK'] = np.nan

    comp_df['FARE'] = np.nan
    comp_df['FAREK'] = np.nan

    comp_df.reset_index(drop=True, inplace=True)
    comp_df.drop(comp_df.columns[0], inplace=True, axis=1)
    comp_df.drop(comp_df.columns[0], inplace=True, axis=1)

    df_ps.reset_index(drop=True, inplace=True)

    df_ps.to_csv("/Users/paul/Downloads/elisapain.csv")
    assert_frame_equal(df_ps, comp_df)

# def test_csvs():
#     pauls_csv = '/Users/paul/Desktop/before_calc.csv'
#     daves_csv = '/Users/paul/Downloads/surveydata_dec2017.csv'
#
#     pauls_df = pd.read_csv(pauls_csv)
#     daves_df = pd.read_csv(daves_csv)
#
#     pauls_df.columns = pauls_df.columns.str.upper()
#     daves_df.columns = daves_df.columns.str.upper()
#
#     pauls_df = pauls_df[['SERIAL', 'FARES_IMP_FLAG_PV', 'FARES_IMP_ELIGIBLE_PV', 'DISCNT_F1_PV', 'DISCNT_PACKAGE_COST_PV',
#                'DISCNT_F2_PV', 'FAGE_PV', 'TYPE_PV', 'UKPORT1_PV', 'UKPORT2_PV', 'UKPORT3_PV',
#                'UKPORT4_PV', 'OSPORT1_PV', 'OSPORT2_PV', 'OSPORT3_PV', 'OSPORT4_PV', 'APD_PV', 'QMFARE_PV', 'DUTY_FREE_PV']].copy()
#     daves_df = daves_df[['SERIAL', 'FARES_IMP_FLAG_PV', 'FARES_IMP_ELIGIBLE_PV', 'DISCNT_F1_PV', 'DISCNT_PACKAGE_COST_PV',
#                'DISCNT_F2_PV', 'FAGE_PV', 'TYPE_PV', 'UKPORT1_PV', 'UKPORT2_PV', 'UKPORT3_PV',
#                'UKPORT4_PV', 'OSPORT1_PV', 'OSPORT2_PV', 'OSPORT3_PV', 'OSPORT4_PV', 'APD_PV', 'QMFARE_PV', 'DUTY_FREE_PV']].copy()
#
#     daves_df = daves_df[daves_df['SERIAL'].isin(pauls_df['SERIAL'])]
#
#     # pauls_df.sort_values('SERIAL', axis=1)
#     # daves_df.sort_values('SERIAL', axis=1)
#
#     assert_frame_equal(pauls_df, daves_df)




