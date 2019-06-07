from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import insert_from_dataframe
from ips.services.calculations import calculate_minimums_weight
from ips.util import process_variables
from ips.util.services_configuration import ServicesConfiguration
from ips.util.services_logging import log


def minimums_weight_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the minimums weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_minimums_weight()

    # Populate Survey Data For Minimums Wt
    idm.populate_survey_data_for_step(run_id, config)

    # Copy Minimums Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Minimums Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_MINIMUMS_SPV',
                              in_id='serial')

    # Update Survey Data with Minimums Wt PVs Output
    idm.update_survey_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = get_survey_data()

    # Calculate Minimums Weight
    output_data, summary_data = \
        calculate_minimums_weight.do_ips_minweight_calculation(df_surveydata=survey_data,
                                                               serial_num='SERIAL',
                                                               shift_weight='SHIFT_WT',
                                                               nr_weight='NON_RESPONSE_WT',
                                                               min_weight='MINS_WT',
                                                               run_id=run_id)

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(output_data)
    insert_from_dataframe(config["sas_ps_table"])(summary_data)

    # Update Survey Data With Minimums Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Minimums Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Minimums Wt Summary
    idm.store_step_summary(run_id, config)
