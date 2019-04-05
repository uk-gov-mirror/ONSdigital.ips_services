from ips.services.calculations import calculate_airmiles
from ips.persistence import data_management as idm
import ips_common_db.sql as db
from ips.util.config.services_configuration import ServicesConfiguration


def airmiles_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the air miles calculation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_air_miles()

    # Populate Survey Data For Air Miles
    idm.populate_survey_data_for_step(run_id, config)

    # Retrieve data from SQL
    survey_data = db.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Air Miles
    survey_data_out = calculate_airmiles.do_ips_airmiles_calculation(df_surveydata=survey_data,
                                                                     var_serial='SERIAL')

    # Insert data to SQL
    db.insert_dataframe_into_table(config["temp_table"], survey_data_out)

    # Update Survey Data with Air Miles Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data with Air Miles Results
    idm.store_survey_data_with_step_results(run_id, config)
