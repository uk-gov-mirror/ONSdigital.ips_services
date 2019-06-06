from ips.persistence.persistence import read_table_values, insert_from_dataframe
from ips.services.calculations import calculate_unsampled_weight
from ips.util.services_configuration import ServicesConfiguration
from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.util import process_variables
from ips.util.services_logging import log


def unsampled_weight_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the unsampled weight steps of the ips process
    Params       : run_id - the id for the current run.
    Returns      : None
    """

    # Load configuration variables
    config = ServicesConfiguration().get_unsampled_weight()

    # Populate Survey Data For Unsampled Wt
    idm.populate_survey_data_for_step(run_id, config)

    # Populate Unsampled Data
    idm.populate_step_data(run_id, config)

    # Copy Unsampled Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Unsampled Wt PV On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_UNSAMPLED_OOH_SPV',
                              in_id='serial')

    # Update Survey Data with Unsampled Wt PV Output
    idm.update_survey_data_with_step_pv_output(config)

    # Copy Unsampled Wt PVs For Unsampled Data
    idm.copy_step_pvs_for_step_data(run_id, config)

    # Apply Unsampled Wt PV On Unsampled Data
    process_variables.process(dataset='unsampled',
                              in_table_name='SAS_UNSAMPLED_OOH_DATA',
                              out_table_name='SAS_UNSAMPLED_OOH_PV',
                              in_id='REC_ID')

    # Update Unsampled Data With PV Output
    idm.update_step_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = get_survey_data()
    unsampled_data = read_table_values(config["data_table"])()

    # Calculate Unsampled Weight
    output_data, summary_data = calculate_unsampled_weight.do_ips_unsampled_weight_calculation(
        df_surveydata=survey_data,
        serial_num='SERIAL',
        shift_weight='SHIFT_WT',
        nr_weight='NON_RESPONSE_WT',
        min_weight='MINS_WT',
        traffic_weight='TRAFFIC_WT',
        out_of_hours_weight="UNSAMP_TRAFFIC_WT",
        df_ustotals=unsampled_data,
        min_count_threshold=30,
        run_id=run_id)

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(output_data)
    insert_from_dataframe(config["sas_ps_table"])(summary_data)

    # Update Survey Data With Unsampled Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Unsampled Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Unsampled Weight Summary
    idm.store_step_summary(run_id, config)
