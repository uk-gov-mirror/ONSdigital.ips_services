from ips.services.calculations import calculate_nonresponse_weight
from ips.util.config.services_configuration import ServicesConfiguration
from ips.persistence import data_management as idm
from ips.util import process_variables
import ips_common_db.sql as db


def non_response_weight_step(run_id):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 26 April 2018 / 2 October 2018
    Purpose      : Runs the non response weight steps of the ips process
    Params       : run_id - the id for the current run.
    Returns      : NA
    """
    # TODO: Move this to the correct workflow
    from ips.persistence import apply_non_response_data_pvs as apply_pvs
    apply_pvs.apply_pvs_to_non_response_data(run_id)
    
    # Load configuration variables
    config = ServicesConfiguration().get_non_response()

    # Populate Survey Data For Non Response Wt
    idm.populate_survey_data_for_step(run_id, config)

    # Copy Non Response Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, config)

    # Apply Non Response Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_NON_RESPONSE_SPV',
                              in_id='serial')

    # Update Survey Data with Non Response Wt PVs Output
    idm.update_survey_data_with_step_pv_output(config)

    # Retrieve data from SQL
    survey_data = db.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    non_response_data = db.get_table_values(config["data_table"])

    # Calculate Non Response Weight
    survey_data_out, summary_data_out = \
        calculate_nonresponse_weight.do_ips_nrweight_calculation(survey_data,
                                                                 non_response_data,
                                                                     'NON_RESPONSE_WT',
                                                                     'SERIAL')

    db.insert_dataframe_into_table(config["temp_table"], survey_data_out)
    db.insert_dataframe_into_table(config["sas_ps_table"], summary_data_out)

    # Update Survey Data With Non Response Wt Results
    idm.update_survey_data_with_step_results(config)

    # Store Survey Data With NonResponse Wt Results
    idm.store_survey_data_with_step_results(run_id, config)

    # Store Non Response Wt Summary
    idm.store_step_summary(run_id, config)
