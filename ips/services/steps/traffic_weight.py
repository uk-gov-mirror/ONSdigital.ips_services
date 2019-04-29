from ips.services.calculations.calculate_traffic_weight import do_ips_trafweight_calculation_with_R
from ips.persistence import data_management as idm
from ips.util.config.services_configuration import ServicesConfiguration
from ips.services.steps import process_variables
import ips_common_db.sql as db


def traffic_weight_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the traffic weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_traffic_weight()

    # Populate Survey Data For Traffic Wt
    idm.populate_survey_data_for_step(run_id, config)

    # Populate Traffic Data
    idm.populate_step_data(run_id, config)

    # Copy Traffic Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Traffic Wt PV On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TRAFFIC_SPV',
                              in_id='serial')

    # Update Survey Data with Traffic Wt PV Output
    idm.update_survey_data_with_step_pv_output(config)

    # Copy Traffic Wt PVs For Traffic Data
    idm.copy_step_pvs_for_step_data(run_id, config)

    # Apply Traffic Wt PV On Traffic Data
    process_variables.process(dataset='traffic',
                              in_table_name='SAS_TRAFFIC_DATA',
                              out_table_name='SAS_TRAFFIC_PV',
                              in_id='REC_ID')

    # Update Traffic Data With Traffic Wt PV Output
    idm.update_step_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = db.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    traffic_data = db.get_table_values(config["data_table"])

    # Calculate Traffic Weight
    output_data, summary_data = do_ips_trafweight_calculation_with_R(survey_data, traffic_data)

    # Insert data to SQL
    db.insert_dataframe_into_table(config["temp_table"], output_data)
    db.insert_dataframe_into_table(config["sas_ps_table"], summary_data)

    # Update Survey Data With Traffic Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Traffic Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Traffic Wt Summary
    idm.store_step_summary(run_id, config)
