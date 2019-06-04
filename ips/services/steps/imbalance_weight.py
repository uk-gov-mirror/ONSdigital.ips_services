from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import insert_from_dataframe
from ips.services.calculations import calculate_imb_weight
from ips.util import process_variables
from ips.util.services_configuration import ServicesConfiguration
from ips.util.services_logging import log


def imbalance_weight_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the imbalance weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_imbalance_weight()

    # Populate Survey Data For Imbalance Wt
    idm.populate_survey_data_for_step(run_id, config)

    # Copy Imbalance Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Imbalance Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_IMBALANCE_SPV',
                              in_id='serial')

    # Update Survey Data With Imbalance Wt PVs Output
    idm.update_survey_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = get_survey_data()

    # Calculate Imbalance Weight
    survey_data_out, summary_data_out = \
        calculate_imb_weight.do_ips_imbweight_calculation(survey_data,
                                                          serial="SERIAL",
                                                          shift_weight="SHIFT_WT",
                                                          non_response_weight="NON_RESPONSE_WT",
                                                          min_weight="MINS_WT",
                                                          traffic_weight="TRAFFIC_WT",
                                                          oo_weight="UNSAMP_TRAFFIC_WT",
                                                          imbalance_weight="IMBAL_WT")

    # Insert data to SQL
    insert_from_dataframe(config["temp_table"])(survey_data_out)
    insert_from_dataframe(config["sas_ps_table"])(summary_data_out)

    # Update Survey Data With Imbalance Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Imbalance Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Imbalance Weight Summary
    idm.store_step_summary(run_id, config)
