from ips.services.calculations import calculate_regional_weights
from ips.persistence import data_management as idm
from ips.util.config.services_configuration import ServicesConfiguration
from ips.services.steps import process_variables
import ips_common_db.sql as db


def regional_weights_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the regional weights steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_regional_weights()

    # Populate Survey Data For Regional Weights
    idm.populate_survey_data_for_step(run_id, config)

    # Copy Regional Weights PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Regional Weights PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_REGIONAL_SPV',
                              in_id='serial')

    # Update Survey Data with Regional Weights PV Output
    idm.update_survey_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = db.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Regional Weights
    survey_data_out = calculate_regional_weights.do_ips_regional_weight_calculation(survey_data,
                                                                                    serial_num='SERIAL',
                                                                                    final_weight='FINAL_WT')

    # Insert data to SQL
    db.insert_dataframe_into_table(config["temp_table"], survey_data_out)

    # Update Survey Data With Regional Weights Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Regional Weights Results
    idm.store_survey_data_with_step_results(run_id, config)
