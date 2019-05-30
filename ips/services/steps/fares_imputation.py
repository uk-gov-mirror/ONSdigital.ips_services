from ips_common.ips_logging import log

from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import insert_from_dataframe
from ips.services.calculations import calculate_fares_imputation
from ips.util import process_variables
from ips.util.config.services_configuration import ServicesConfiguration
from ...util.sas_random import seed


def fares_imputation_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the fares imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_fares_imputation()

    # Populate Survey Data For Fares Imputation
    idm.populate_survey_data_for_step(run_id, config)

    # Copy Fares Imp PVs For Survey Data
    log.debug("before copy_step_pvs_for_survey_data")
    idm.copy_step_pvs_for_survey_data(run_id, config)
    log.debug("after copy_step_pvs_for_survey_data")

    # Apply Fares Imp PVs On Survey Data
    log.debug("before process")
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_FARES_SPV',
                              in_id='serial')
    log.debug("after process")

    # Update Survey Data with Fares Imp PV Output
    log.debug("before update_survey_data_with_step_pv_output")
    idm.update_survey_data_with_step_pv_output(config)
    log.debug("after update_survey_data_with_step_pv_output")

    # Retrieve data from SQL
    survey_data = get_survey_data()

    # Calculate Fares Imputation
    log.debug("before calculate_fares_imputation")
    survey_data_out = calculate_fares_imputation.do_ips_fares_imputation(survey_data,
                                                                         var_serial='SERIAL',
                                                                         num_levels=9,
                                                                         measure='mean')
    log.debug("after calculate_fares_imputation")

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(survey_data_out)

    # Update Survey Data With Fares Imp Results

    log.debug("before update_survey_data_with_step_results")
    idm.update_survey_data_with_step_results(config)
    log.debug("after update_survey_data_with_step_results")

    # Store Survey Data With Fares Imp Results
    idm.store_survey_data_with_step_results(run_id, config)
