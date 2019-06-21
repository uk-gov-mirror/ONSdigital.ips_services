import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal


def test_during():
    sas_after_impute = '/Users/paul/Desktop/ONSData/sas_impute_from.csv'
    python_after_impute = "/Users/paul/Desktop/ONSData/ips_impute_segment-python-impute-from.csv"

    sas_df = pd.read_csv(sas_after_impute)
    python_df = pd.read_csv(python_after_impute)

    sas_df = sas_df.sort_values('SERIAL')
    python_df = python_df.sort_values('SERIAL')

    python_df.reset_index(drop=True, inplace=True)
    sas_df.reset_index(drop=True, inplace=True)

    python_df.drop(python_df.columns[0], inplace=True, axis=1)

    df_ps = python_df[
        [
            'SERIAL',
            'INTDATE', 'TYPE_PV',
            'UKPORT1_PV', 'OSPORT1_PV',
            'OPERA_PV',
            'DVFARE'
        ]
    ]

    df_ps1 = sas_df[
        [
            'SERIAL',
            'INTDATE', 'TYPE_PV',
            'UKPORT1_PV', 'OSPORT1_PV',
            'OPERA_PV',
            'DVFARE'
        ]
    ]

    assert_frame_equal(df_ps, df_ps1)
    # assert_frame_equal(sas_df, python_df)


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


def test_before_el():
    sas_csv = '/Users/paul/Desktop/ONSData/befre_apply_full_set.csv'
    python_csv = "/Users/paul/Desktop/ONSData/before-compute.csv"

    sas_df = pd.read_csv(sas_csv)
    python_df = pd.read_csv(python_csv)

    sas_df = sas_df.sort_values('SERIAL')
    python_df = python_df.sort_values('SERIAL')

    python_df.reset_index(drop=True, inplace=True)
    sas_df.reset_index(drop=True, inplace=True)

    python_df.drop(python_df.columns[0], inplace=True, axis=1)

    assert_frame_equal(sas_df, python_df, check_like=True)


def test_subset_before_el():
    sas_csv = '/Users/paul/Desktop/ONSData/befre_apply_subset.csv'
    python_csv = "/Users/paul/Desktop/ONSData/spend.csv"

    sas_df = pd.read_csv(sas_csv)
    python_df = pd.read_csv(python_csv)

    # df_ps = python_df[
    #     [
    #         'SERIAL', 'INTDATE', 'FARES_IMP_FLAG_PV', 'FARES_IMP_ELIGIBLE_PV', 'FARE', 'DVFARE', 'FAGE_PV', 'BABYFARE',
    #         'APD_PV', 'CHILDFARE', 'DVPACKAGE', 'DISCNT_F2_PV', 'QMFARE_PV', 'DISCNT_PACKAGE_COST_PV', 'DVPACKCOST',
    #         'DVEXPEND', 'BEFAF', 'SPEND', 'DVPERSONS', 'SPENDIMPREASON', 'PACKAGE', 'DUTY_FREE_PV'
    #     ]
    # ]

    df_ps = python_df[
        [
            'SERIAL', 'DVPACKAGE', 'DISCNT_PACKAGE_COST_PV', 'DVPACKCOST', 'DVEXPEND', 'BEFAF', 'SPEND', 'DISCNT_PACKAGE_COST_PV', 'DVPERSONS', 'SPENDIMPREASON', 'PACKAGE', 'DUTY_FREE_PV'
        ]
    ]

    df_sas = sas_df[
        [
            'SERIAL', 'DVPACKAGE', 'DISCNT_PACKAGE_COST_PV', 'DVPACKCOST', 'DVEXPEND', 'BEFAF', 'SPEND', 'DISCNT_PACKAGE_COST_PV', 'DVPERSONS', 'SPENDIMPREASON', 'PACKAGE', 'DUTY_FREE_PV'
        ]
    ]

    sas_df = df_sas.sort_values('SERIAL')
    python_df = df_ps.sort_values('SERIAL')

    python_df.reset_index(drop=True, inplace=True)
    sas_df.reset_index(drop=True, inplace=True)

    # python_df.drop(python_df.columns[0], inplace=True, axis=1)

    assert_frame_equal(sas_df, python_df, check_like=True, check_dtype=False)


def test_fares():
    from ips.persistence import data_management as idm
    from ips.services.calculations import calculate_fares_imputation
    from ips.persistence.persistence import insert_from_dataframe
    from ips.util.services_configuration import ServicesConfiguration
    from ips.persistence.persistence import select_data
    survey_subsample_table = 'SURVEY_SUBSAMPLE'
    config = ServicesConfiguration().get_fares_imputation()
    run_id = 'h3re-1s-y0ur-run-1d'

    # Load survey data
    survey_data = pd.read_csv("/Users/paul/Desktop/ONSData/fares_calculation_input.csv")

    # Run fares calculation and subsequent db steps
    survey_data_out = calculate_fares_imputation.do_ips_fares_imputation(survey_data,
                                                                         var_serial='SERIAL',
                                                                         num_levels=9,
                                                                         measure='mean')
    insert_from_dataframe(config["temp_table"])(survey_data_out)
    idm.update_survey_data_with_step_results(config)
    idm.store_survey_data_with_step_results(run_id, config)

    # Start testing shizznizz
    # TODO --->
    output_columns = ['SERIAL', 'FAREK', 'SPEND', 'SPENDIMPREASON']

    # Create comparison survey dataframes
    survey_subsample = select_data("*", survey_subsample_table, "RUN_ID", run_id)

    survey_results = survey_subsample[output_columns].copy()
    survey_expected = pd.read_csv("data/calculations/december_2017/stay/surveydata_dec2017.csv")
    survey_expected = survey_expected[output_columns].copy()

    # pandas.testing.faff
    survey_results.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_results.index = range(0, len(survey_results))

    survey_expected.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_expected.index = range(0, len(survey_expected))

    # TODO: run this and it should produce a result of...
    assert_frame_equal(survey_results, survey_expected, check_dtype=False, check_less_precise=True)
