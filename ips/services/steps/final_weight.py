from ips.persistence import data_management as idm
from ips.persistence.data_management import get_survey_data
from ips.persistence.persistence import insert_from_dataframe
from ips.services.calculations import calculate_final_weight
from ips.util.config.services_configuration import ServicesConfiguration


def final_weight_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the final weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    config = ServicesConfiguration().get_final_weight()

    # Populate Survey Data For Final Wt
    idm.populate_survey_data_for_step(run_id, config)

    # Retrieve data from SQL
    survey_data = get_survey_data()

    # Calculate Final Weight
    survey_data_out, summary_data_out = \
        calculate_final_weight.do_ips_final_wt_calculation(survey_data,
                                                           serial_num='SERIAL',
                                                           shift_weight='SHIFT_WT',
                                                           non_response_weight='NON_RESPONSE_WT',
                                                           min_weight='MINS_WT',
                                                           traffic_weight='TRAFFIC_WT',
                                                           unsampled_weight='UNSAMP_TRAFFIC_WT',
                                                           imbalance_weight='IMBAL_WT',
                                                           final_weight='FINAL_WT')

    # Insert data to SQL
    import ips.services.dataimport.schemas.sas_ps_final as schema
    insert_from_dataframe(config["temp_table"])(survey_data_out)
    insert_from_dataframe(config["sas_ps_table"], dtype=schema.get_schema())(summary_data_out)

    # Update Survey Data With Final Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With Final Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Final Weight Summary
    idm.store_step_summary(run_id, config)
