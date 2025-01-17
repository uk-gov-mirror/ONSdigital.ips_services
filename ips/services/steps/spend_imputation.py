from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import insert_from_dataframe
from ips.services.calculations import calculate_spend_imputation
from ips.util import process_variables
from ips.util.services_configuration import ServicesConfiguration


def spend_imputation_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the spend imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_spend_imputation()

    # Populate Survey Data For Spend Imputation
    idm.populate_survey_data_for_step(run_id, config)

    # Copy Spend Imp PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Spend Imp PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SPEND_SPV',
                              in_id='serial')

    # Update Survey Data with Spend Imp PV Output
    idm.update_survey_data_with_step_pv_output(config)

    if ServicesConfiguration().sas_pur2_pv():
        idm.pur2_pv()

    # Retrieve data from SQL
    survey_data = get_survey_data()

    # Calculate Spend Imputation
    survey_data_out = calculate_spend_imputation.do_ips_spend_imputation(survey_data,
                                                                         var_serial="SERIAL",
                                                                         measure="mean")

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(survey_data_out)

    # Update Survey Data With Spend Imp Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Spend Imp Results
    idm.store_survey_data_with_step_results(run_id, config)
