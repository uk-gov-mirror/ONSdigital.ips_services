from ips.util.services_logging import log
from ips.persistence.persistence import delete_from_table, insert_into_table, select_data, read_table_values
from ips.persistence.persistence import execute_sql as exec_sql

SURVEY_SUBSAMPLE_TABLE = "SURVEY_SUBSAMPLE"
SAS_SURVEY_SUBSAMPLE_TABLE = "SAS_SURVEY_SUBSAMPLE"
SAS_PROCESS_VARIABLES_TABLE = "SAS_PROCESS_VARIABLE"

clear_subsample = delete_from_table(SAS_SURVEY_SUBSAMPLE_TABLE)
execute_sql = exec_sql()
get_survey_data = read_table_values(SAS_SURVEY_SUBSAMPLE_TABLE)

COLUMNS_TO_MOVE = [
    'SERIAL', 'AGE', 'AM_PM_NIGHT', 'ANYUNDER16', 'APORTLATDEG', 'APORTLATMIN', 'APORTLATSEC',
    'APORTLATNS', 'APORTLONDEG', 'APORTLONMIN', 'APORTLONSEC', 'APORTLONEW', 'ARRIVEDEPART',
    'BABYFARE', 'BEFAF', 'CHANGECODE', 'CHILDFARE', 'COUNTRYVISIT', 'CPORTLATDEG',
    'CPORTLATMIN', 'CPORTLATSEC', 'CPORTLATNS', 'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONSEC',
    'CPORTLONEW', 'INTDATE', 'DAYTYPE', 'DIRECTLEG', 'DVEXPEND', 'DVFARE', 'DVLINECODE',
    'DVPACKAGE', 'DVPACKCOST', 'DVPERSONS', 'DVPORTCODE', 'EXPENDCODE', 'EXPENDITURE', 'FARE',
    'FAREK', 'FLOW', 'HAULKEY', 'INTENDLOS', 'KIDAGE', 'LOSKEY', 'MAINCONTRA', 'MIGSI',
    'INTMONTH', 'NATIONALITY', 'NATIONNAME', 'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4',
    'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8', 'NUMADULTS', 'NUMDAYS', 'NUMNIGHTS',
    'NUMPEOPLE', 'PACKAGEHOL', 'PACKAGEHOLUK', 'PERSONS', 'PORTROUTE', 'PACKAGE',
    'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC', 'PROUTELATNS', 'PROUTELONDEG',
    'PROUTELONMIN', 'PROUTELONSEC', 'PROUTELONEW', 'PURPOSE', 'QUARTER', 'RESIDENCE',
    'RESPNSE', 'SEX', 'SHIFTNO', 'SHUTTLE', 'SINGLERETURN', 'TANDTSI', 'TICKETCOST',
    'TOWNCODE1', 'TOWNCODE2', 'TOWNCODE3', 'TOWNCODE4', 'TOWNCODE5', 'TOWNCODE6', 'TOWNCODE7',
    'TOWNCODE8', 'TRANSFER', 'UKFOREIGN', 'VEHICLE', 'VISITBEGAN', 'WELSHNIGHTS', 'WELSHTOWN',
    'AM_PM_NIGHT_PV', 'APD_PV', 'ARRIVEDEPART_PV', 'CROSSINGS_FLAG_PV', 'STAYIMPCTRYLEVEL1_PV',
    'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV', 'STAYIMPCTRYLEVEL4_PV', 'DAY_PV',
    'DISCNT_F1_PV', 'DISCNT_F2_PV', 'DISCNT_PACKAGE_COST_PV', 'DUR1_PV', 'DUR2_PV',
    'DUTY_FREE_PV', 'FAGE_PV', 'FARES_IMP_ELIGIBLE_PV', 'FARES_IMP_FLAG_PV', 'FLOW_PV',
    'FOOT_OR_VEHICLE_PV', 'HAUL_PV', 'IMBAL_CTRY_FACT_PV', 'IMBAL_CTRY_GRP_PV',
    'IMBAL_ELIGIBLE_PV', 'IMBAL_PORT_FACT_PV', 'IMBAL_PORT_GRP_PV', 'IMBAL_PORT_SUBGRP_PV',
    'LOS_PV', 'LOSDAYS_PV', 'MIG_FLAG_PV', 'MINS_CTRY_GRP_PV', 'MINS_CTRY_PORT_GRP_PV',
    'MINS_FLAG_PV', 'MINS_NAT_GRP_PV', 'MINS_PORT_GRP_PV', 'MINS_QUALITY_PV', 'NR_FLAG_PV',
    'NR_PORT_GRP_PV', 'OPERA_PV', 'OSPORT1_PV', 'OSPORT2_PV', 'OSPORT3_PV', 'OSPORT4_PV',
    'PUR1_PV', 'PUR2_PV', 'PUR3_PV', 'PURPOSE_PV', 'QMFARE_PV', 'RAIL_EXERCISE_PV',
    'RAIL_IMP_ELIGIBLE_PV', 'SAMP_PORT_GRP_PV', 'SHIFT_FLAG_PV', 'SHIFT_PORT_GRP_PV',
    'STAY_IMP_ELIGIBLE_PV', 'STAY_IMP_FLAG_PV', 'STAY_PURPOSE_GRP_PV', 'TOWNCODE_PV',
    'TYPE_PV', 'UK_OS_PV', 'UKPORT1_PV', 'UKPORT2_PV', 'UKPORT3_PV', 'UKPORT4_PV',
    'UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'WEEKDAY_END_PV', 'DIRECT', 'EXPENDITURE_WT',
    'EXPENDITURE_WTK', 'OVLEG', 'SPEND', 'SPEND1', 'SPEND2', 'SPEND3', 'SPEND4', 'SPEND5',
    'SPEND6', 'SPEND7', 'SPEND8', 'SPEND9', 'SPENDIMPREASON', 'SPENDK', 'STAY', 'STAYK',
    'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K', 'STAY9K',
    'STAYTLY', 'STAY_WT', 'STAY_WTK', 'UKLEG', 'VISIT_WT', 'VISIT_WTK', 'SHIFT_WT',
    'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT',
    'FAREKEY', 'TYPEINTERVIEW'
]


def nullify_survey_subsample_values(run_id: str, pv_values):
    """
    Author       : Elinor Thorne
    Date         : Apr 2018
    Purpose      : Updates required columns to null
    Parameters   : NA
    Returns      : NA
    """

    # insert_into_pv_bytes(Run_ID=run_id, PV_Bytes=str(code_bytes), PV_ID=pv_id)

    # Construct string for SQL statement
    columns_to_null = []
    for item in pv_values:
        columns_to_null.append(item + " = null")
    columns_to_null = ", ".join(map(str, columns_to_null))

    # Create SQL Statement
    sql = f"""UPDATE {SURVEY_SUBSAMPLE_TABLE} 
        SET {columns_to_null}
        WHERE RUN_ID = '{run_id}'"""

    # Execute and commits the SQL command

    execute_sql(sql)


def move_survey_subsample_to_sas_table(run_id, step_name):
    """
    Author       : Elinor Thorne
    Date         : Apr 2018
    Purpose      : Moves data to temporary location
    Parameters   : NA
    Returns      : NA
    """

    columns = ["" + col + "" for col in COLUMNS_TO_MOVE]
    columns = ','.join(columns)

    # Assign RESPNSE condition to step
    if step_name == "TRAFFIC_WEIGHT" or step_name == "UNSAMPLED_WEIGHT":
        respnse = "BETWEEN 1 and 2"
    else:
        respnse = "BETWEEN 1 and 6"

    sql = f"""
        INSERT INTO {SAS_SURVEY_SUBSAMPLE_TABLE}
        ({columns})
        (SELECT {columns}
        FROM {SURVEY_SUBSAMPLE_TABLE}
        WHERE RUN_ID = '{run_id}'
        AND SERIAL NOT LIKE '9999%%'
        AND RESPNSE {respnse})
    """

    execute_sql(sql)


def populate_survey_data_for_step(run_id, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : 13 Apr 2018
    Purpose      : Populates survey_data in preparation for step
    Parameters   : conn - connection object pointing at the database
                 : run_id -
                 : step -
    Returns      : NA
    """

    # Cleanse tables as applicable
    clear_subsample()

    for table in step_configuration["delete_tables"]:
        delete_from_table(table)()

    nullify_survey_subsample_values(run_id, step_configuration["nullify_pvs"])
    move_survey_subsample_to_sas_table(run_id, step_configuration["name"])


def populate_step_data(run_id, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Populate step
    Parameters   : run_id -
                 : conn -
                 : step -
    Returns      : NA
    """

    # Assign variables
    table = step_configuration["table_name"]
    data_table = step_configuration["data_table"]
    columns = step_configuration["insert_to_populate"]
    cols = ", ".join(map(str, columns))

    # Construct string for SQL statement
    calc_cols = ["CALC." + col for col in columns]
    calc_columns = ", ".join(map(str, calc_cols))

    # Cleanse temp table
    delete_from_table(data_table)()

    # Create and execute SQL statement
    sql = f"""
        INSERT INTO {data_table}
            ({cols})
        SELECT {calc_columns}
        FROM {table} AS CALC
        WHERE RUN_ID = '{run_id}'
    """

    execute_sql(sql)


def copy_step_pvs_for_survey_data(run_id, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Copy step process variable data
    Parameters   : run_id -
                 : conn -
                 : step -
    Returns      : NA
    """

    spv_table = step_configuration["spv_table"]

    # Cleanse tables
    delete_from_table(SAS_PROCESS_VARIABLES_TABLE)()
    delete_from_table(spv_table)()

    step = step_configuration["name"]

    # Loops through the pv's and inserts them into the process variable table
    count = 0
    for item in step_configuration["pv_columns"]:
        count = count + 1
        sql = f"""
        INSERT INTO {SAS_PROCESS_VARIABLES_TABLE}
            (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, {count}
            FROM PROCESS_VARIABLE_PY AS PV WHERE PV.RUN_ID = '{run_id}' 
            AND UPPER(PV.PV_NAME) IN ('{item}'))
        """
        execute_sql(sql)


def update_survey_data_with_step_pv_output(step_configuration):
    """
    Author       : Elinor Thorne / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Updates survey_data with the process variable outputs
    Parameters   : conn - connection object pointing at the database
                 : step -
    Returns      : NA
    """

    # Assign variables
    spv_table = step_configuration["spv_table"]

    # Construct string for SQL statement
    cols = [item.replace("'", "") for item in step_configuration["pv_columns"]]
    cols = ["SSS." + item + " = CALC." + item for item in cols]
    set_statement = ", ".join(map(str, cols))

    sql = f"""
        UPDATE {SAS_SURVEY_SUBSAMPLE_TABLE} AS SSS, {spv_table} AS CALC
            SET {set_statement}
            WHERE SSS.SERIAL = CALC.SERIAL
        """

    execute_sql(sql)

    # Cleanse temp tables
    delete_from_table(SAS_PROCESS_VARIABLES_TABLE)()
    delete_from_table(spv_table)()

    # code specific to minimums weight function/step
    # TODO: consider moving this out to another function called by minimum weight
    if step_configuration["name"] == "MINIMUMS_WEIGHT":
        delete_from_table(step_configuration["temp_table"])()
        delete_from_table(step_configuration["sas_ps_table"])()


def copy_step_pvs_for_step_data(run_id, step_configuration):
    """
    Author       : Elinor Thorne / Nassir Mohammad
    Date         : July 2018
    Purpose      : Copies the process variables for the step.
    Parameters   : run_id -
                 : conn - connection object pointing at the database.
                 : step -
    Returns      : NA
    """

    # Cleanse temp tables
    delete_from_table(SAS_PROCESS_VARIABLES_TABLE)()
    delete_from_table(step_configuration["pv_table"])()

    # Construct and execute SQL statements as applicable
    if step_configuration["name"] == 'UNSAMPLED_WEIGHT':
        order = step_configuration["order"] + 1
        for item in step_configuration["pv_columns2"]:
            sql = (f"""
                 INSERT INTO {SAS_PROCESS_VARIABLES_TABLE}
                 (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)
                     (SELECT pv.PV_NAME, pv.PV_DEF, {order}
                     FROM PROCESS_VARIABLE_PY AS pv
                     WHERE pv.RUN_ID = '{run_id}'
                     AND UPPER(pv.PV_NAME) in ('{item}'))
                 """)
            execute_sql(sql)
            order = order + 1
    else:
        cols = []
        for item in step_configuration["pv_columns2"]:
            cols.append("'" + item + "'")
        pv_columns = ", ".join(map(str, cols))

        sql = f"""
            INSERT INTO {SAS_PROCESS_VARIABLES_TABLE}
            (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)
                (SELECT pv.PV_NAME, pv.PV_DEF, {step_configuration["order"]}
                FROM PROCESS_VARIABLE_PY AS pv
                WHERE pv.RUN_ID = '{run_id}'
                AND UPPER(pv.PV_NAME) in ({pv_columns})) 
        """
        execute_sql(sql)


def update_step_data_with_step_pv_output(step_configuration):
    """
    Author       : Elinor Thorne / Nassir Mohammad
    Date         : July 2018
    Purpose      : Updates data with the process variable output.
    Parameters   : conn - connection object pointing at the database.
                 : step -
    Returns      : NA
    """

    # Construct string for SQL statement
    cols = [item.replace("'", "") for item in step_configuration["pv_columns2"]]
    cols = ["SSS." + item + " = CALC." + item for item in cols]
    set_statement = ", ".join(map(str, cols))

    # Construct and execute SQL statement
    pv_table = step_configuration["pv_table"]
    data_table = step_configuration["data_table"]

    sql = f"""
        UPDATE {data_table} as SSS, {pv_table} as CALC
            SET {set_statement}
            WHERE SSS.REC_ID = CALC.REC_ID
            """

    execute_sql(sql)

    # Cleanse temporary tables
    delete_from_table(step_configuration["pv_table"])()
    delete_from_table(step_configuration["temp_table"])()
    delete_from_table(SAS_PROCESS_VARIABLES_TABLE)()
    delete_from_table(step_configuration["sas_ps_table"])()


def sql_update_statement(table_to_update_from, columns_to_update):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Constructs SQL update statement
    Parameters   : step -
    Returns      : String - SQL update statement
    """
    # Construct SET string
    cols = ["SSS." + item + ' = temp.' + item for item in columns_to_update]
    columns = " , ".join(cols)

    # Construct SQL statement and execute

    sql = f"""
        UPDATE {SAS_SURVEY_SUBSAMPLE_TABLE} as SSS, {table_to_update_from} as temp
            SET {columns}
                WHERE SSS.SERIAL = temp.SERIAL
            """

    return sql


def update_green(table, results_columns):
    sql1 = sql_update_statement(table, results_columns)
    execute_sql(sql1)


def update_imbalance_weights(table, results_columns):
    sql1 = sql_update_statement(table, results_columns)
    sql2 = f"""
        UPDATE {SAS_SURVEY_SUBSAMPLE_TABLE}
            SET IMBAL_WT = 1.00
                WHERE IMBAL_WT IS NULL
            """
    execute_sql(sql1)
    execute_sql(sql2)


def update_stay_imputation(table, results_columns):
    sql1 = sql_update_statement(table, results_columns)

    sql2 = """
        update SAS_SURVEY_SUBSAMPLE
            SET STAY = NUMNIGHTS
                WHERE SERIAL NOT IN (SELECT SERIAL FROM SAS_STAY_IMP)
    """
    execute_sql(sql1)
    execute_sql(sql2)


def update_spend_imputation(table, results_columns):

    sql1 = """
        UPDATE SAS_SURVEY_SUBSAMPLE AS SSS, SAS_SPEND_IMP AS SSI
            SET SSS.SPEND = SSI.NEWSPEND
            WHERE SSS.SERIAL = SSI.SERIAL
                AND SSS.SERIAL IN (SELECT SERIAL from SAS_SPEND_IMP where NEWSPEND >= 0)
    """

    sql2 = sql_update_statement(table, results_columns)
    execute_sql(sql1)
    execute_sql(sql2)


def update_others(table):
    sql1 = f"""
        UPDATE {SAS_SURVEY_SUBSAMPLE_TABLE}, {table}
            SET {SAS_SURVEY_SUBSAMPLE_TABLE}.SPEND = {table}.SPEND
            WHERE ({SAS_SURVEY_SUBSAMPLE_TABLE}.SERIAL = {table}.SERIAL)
                AND {table}.SPEND >=0
        """
    execute_sql(sql1)


def update_survey_data_with_step_results(step_configuration):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Updates survey data with the results
    Parameters   : conn - connection object pointing at the database
                 : step -
    Returns      : NA
    """

    valid_steps = [
        "SHIFT_WEIGHT", "NON_RESPONSE", "MINIMUMS_WEIGHT", "TRAFFIC_WEIGHT",
        "UNSAMPLED_WEIGHT", "FINAL_WEIGHT", "IMBALANCE_WEIGHT", "FARES_IMPUTATION",
        "REGIONAL_WEIGHTS", "TOWN_AND_STAY_EXPENDITURE", "RAIL_IMPUTATION",
        "STAY_IMPUTATION", "SPEND_IMPUTATION", "AIR_MILES"
    ]

    step = step_configuration["name"]

    if step not in valid_steps:
        log.error("Invalid step in update_survey_data_with_step_results: likely a configuration error")
        raise NameError("Invalid step")

    table = step_configuration["temp_table"]
    results_columns = step_configuration["results_columns"]

    dispatcher = {
        "SHIFT_WEIGHT": update_green,
        "NON_RESPONSE": update_green,
        "MINIMUMS_WEIGHT": update_green,
        "TRAFFIC_WEIGHT": update_green,
        "UNSAMPLED_WEIGHT": update_green,
        "FINAL_WEIGHT": update_green,
        "FARES_IMPUTATION": update_green,
        "REGIONAL_WEIGHTS": update_green,
        "TOWN_AND_STAY_EXPENDITURE": update_green,
        "AIR_MILES": update_green,
        "IMBALANCE_WEIGHT": update_imbalance_weights,
        "STAY_IMPUTATION": update_stay_imputation,
        "SPEND_IMPUTATION": update_spend_imputation
    }

    update_step_results = dispatcher.get(step)

    if update_step_results is not None:
        update_step_results(table, results_columns)
    else:
        update_others(table)

    delete_from_table(table)()


def store_survey_data_with_step_results(run_id, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Stores the survey data with the results
    Parameters   : run_id -
                 : conn - connection object pointing at the database.
    Returns      : NA
    """

    step = step_configuration["name"]
    cols = step_configuration["nullify_pvs"]

    # Add additional column to two steps
    if (step == "SPEND_IMPUTATION") or (step == "RAIL_IMPUTATION"):
        cols.append("SPEND")

    cols = ["SS." + item + " = SSS." + item for item in cols]
    set_statement = " , ".join(cols)

    # Create SQL statement and execute
    sql = f"""
        UPDATE {SURVEY_SUBSAMPLE_TABLE} AS SS, {SAS_SURVEY_SUBSAMPLE_TABLE} AS SSS
            SET {set_statement}
                WHERE SS.SERIAL = SSS.SERIAL AND SS.RUN_ID = '{run_id}'
    """

    execute_sql(sql)

    # Cleanse summary and subsample tables as applicable
    ps_tables_to_delete = [
        "SHIFT_WEIGHT",
        "NON_RESPONSE",
        "MINIMUMS_WEIGHT",
        "TRAFFIC_WEIGHT",
        "UNSAMPLED_WEIGHT",
        "IMBALANCE_WEIGHT",
        "FINAL_WEIGHT"
    ]

    if step in ps_tables_to_delete:
        delete_from_table(step_configuration["ps_table"])(run_id=run_id)

    delete_from_table(SAS_SURVEY_SUBSAMPLE_TABLE)()


def store_step_summary(run_id, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Stores the summary data
    Parameters   : run_id
                 : conn - connection object pointing at the database
                 : step -
    Returns      : NA
    """

    # Assign variables
    ps_table = step_configuration["ps_table"]
    sas_ps_table = step_configuration["sas_ps_table"]

    # Cleanse summary table as applicable
    delete_from_table(ps_table)(run_id=run_id)

    # Create selection string
    selection = [col for col in step_configuration["ps_columns"] if col != "RUN_ID"]
    columns = " , ".join(step_configuration["ps_columns"])
    selection = " , ".join(selection)

    # Create and execute SQL statement
    sql = f"""
        INSERT INTO {ps_table} ({columns})
            SELECT '{run_id}', {selection} FROM {sas_ps_table}
    """

    execute_sql(sql)

    # Cleanse temporary summary table
    delete_from_table(sas_ps_table)()


def is_valid_run_id(run_id: str) -> bool:

    df = select_data(column_name="run_id", table='SURVEY_SUBSAMPLE', condition1="RUN_ID", condition2=run_id)

    if df.empty:
        return False
    else:
        return True
