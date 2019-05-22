from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import insert_from_dataframe
from ips.services.calculations import calculate_rail_imputation
from ips.util import process_variables
from ips.util.config.services_configuration import ServicesConfiguration


def rail_imputation_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the rail imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_rail_imputation()

    # Populate Survey Data For Rail Imputation
    idm.populate_survey_data_for_step(run_id, config)

    # Copy Rail Imp PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Rail Imp PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_RAIL_SPV',
                              in_id='serial')

    # Update Survey Data with Rail Imp PV Output
    idm.update_survey_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = get_survey_data()

    # Calculate Rail Imputation
    survey_data_out = calculate_rail_imputation.do_ips_railex_imp(survey_data,
                                                                  var_serial='SERIAL',
                                                                  var_final_weight='FINAL_WT',
                                                                  minimum_count_threshold=30,
                                                                  run_id=run_id)

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(survey_data_out)

    # Update Survey Data With Rail Imp Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Rail Imp Results
    idm.store_survey_data_with_step_results(run_id, config)
