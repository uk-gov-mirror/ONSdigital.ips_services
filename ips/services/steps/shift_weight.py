from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import read_table_values, insert_from_dataframe
from ips.services.calculations import calculate_shift_weight
from ips.util import process_variables
from ips.util.services_configuration import ServicesConfiguration
from ips.util.services_logging import log


def shift_weight_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 26 April 2018 / 2 October 2018
    Purpose      : Runs the shift weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_shift_weight()

    # Populate Survey Data For Shift Wt
    idm.populate_survey_data_for_step(run_id, config)

    # Populate Shift Data
    idm.populate_step_data(run_id, config)

    # Copy Shift Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    log.debug("shift_weight: Apply Shift Wt PVs On Survey Data")
    # Apply Shift Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SHIFT_SPV',
                              in_id='serial')

    # Update Survey Data with Shift Wt PV Output
    idm.update_survey_data_with_step_pv_output(config)

    # Copy Shift Wt PVs For Shift Data
    idm.copy_step_pvs_for_step_data(run_id, config)

    log.debug("shift_weight: Apply Shift Wt PVs On Shift Data")
    # Apply Shift Wt PVs On Shift Data
    process_variables.process(dataset='shift',
                              in_table_name='SAS_SHIFT_DATA',
                              out_table_name='SAS_SHIFT_PV',
                              in_id='REC_ID')

    # Update Shift Data with PVs Output
    idm.update_step_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = get_survey_data()
    shift_data = read_table_values(config["data_table"])()

    log.debug("shift_weight: Calculate Shift Weight")
    # Calculate Shift Weight
    survey_data_out, summary_data_out = \
        calculate_shift_weight.do_ips_shift_weight_calculation(survey_data,
                                                               shift_data,
                                                               serial_number='SERIAL',
                                                               shift_weight='SHIFT_WT', run_id=run_id)

    # Insert data to SQL
    # round columns to avoid truncation
    survey_data_out.SHIFT_WT = survey_data_out.SHIFT_WT.round(3)
    insert_from_dataframe(config["temp_table"])(survey_data_out)

    summary_data_out.SUM_SH_WT = summary_data_out.SUM_SH_WT.round(3)
    insert_from_dataframe(config["sas_ps_table"])(summary_data_out)

    # Update Survey Data With Shift Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Shift Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Shift Wt Summary
    idm.store_step_summary(run_id, config)
