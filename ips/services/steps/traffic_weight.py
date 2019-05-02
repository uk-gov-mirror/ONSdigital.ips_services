from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import read_table_values, insert_from_dataframe
from ips.services.calculations.calculate_traffic_weight import do_ips_trafweight_calculation_with_r
from ips.util import process_variables
from ips.util.config.services_configuration import ServicesConfiguration


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
    survey_data = get_survey_data()
    traffic_data = read_table_values(config["data_table"])()

    # Calculate Traffic Weight
    output_data, summary_data = do_ips_trafweight_calculation_with_r(survey_data, traffic_data)

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(output_data)
    insert_from_dataframe(config["sas_ps_table"])(summary_data)

    # Update Survey Data With Traffic Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Traffic Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Traffic Wt Summary
    idm.store_step_summary(run_id, config)
