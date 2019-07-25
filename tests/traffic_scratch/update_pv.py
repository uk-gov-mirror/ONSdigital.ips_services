import time
import ips.persistence.sql as db
from ips.persistence.persistence import execute_sql as exec_sql

execute_sql = exec_sql()

run_id = 'update_samp_port_grp_pv'
month = 'Q3'
year = '2017'

start_time = time.time()

def setup_pv():
    db.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
    df = db.select_data('*', 'PROCESS_VARIABLE_PY', 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)

    update_traffic_weight_pv()


def update_traffic_weight_pv():
    samp_port_grp_pv = """
    'if row[''PORTROUTE''] in (111, 113, 119, 161, 171):
        row[''SAMP_PORT_GRP_PV''] = ''A111''
    elif row[''PORTROUTE''] in (121, 123, 162, 172):
        row[''SAMP_PORT_GRP_PV''] = ''A121''
    elif row[''PORTROUTE''] in (131, 132, 133, 134, 135, 163, 173):
        row[''SAMP_PORT_GRP_PV''] = ''A131''
    elif row[''PORTROUTE''] in (141, 142, 143, 144, 145, 164, 174):
        row[''SAMP_PORT_GRP_PV''] = ''A141''
    elif row[''PORTROUTE''] in (151, 152, 153, 154, 165, 175):
        row[''SAMP_PORT_GRP_PV''] = ''A151''
    elif row[''PORTROUTE''] in (181, 183, 189):
        row[''SAMP_PORT_GRP_PV''] = ''A181''
    elif row[''PORTROUTE''] in (191, 192, 193, 199):
        row[''SAMP_PORT_GRP_PV''] = ''A191''
    elif row[''PORTROUTE''] in (201, 202, 203, 204):
        row[''SAMP_PORT_GRP_PV''] = ''A201''
    elif row[''PORTROUTE''] in (211, 213, 219):
        row[''SAMP_PORT_GRP_PV''] = ''A211''
    elif row[''PORTROUTE''] in (221, 223):
        row[''SAMP_PORT_GRP_PV''] = ''A221''
    elif row[''PORTROUTE''] in (231, 232, 234):
        row[''SAMP_PORT_GRP_PV''] = ''A231''
    elif row[''PORTROUTE''] in (241, 243, 249):
        row[''SAMP_PORT_GRP_PV''] = ''A241''
    elif row[''PORTROUTE''] in (311, 313, 319):
        row[''SAMP_PORT_GRP_PV''] = ''A311''
    elif row[''PORTROUTE''] == 321:
        row[''SAMP_PORT_GRP_PV''] = ''A321''
    elif row[''PORTROUTE''] == 331:
        row[''SAMP_PORT_GRP_PV''] = ''A331''
    elif row[''PORTROUTE''] == 351:
        row[''SAMP_PORT_GRP_PV''] = ''A351''
    elif row[''PORTROUTE''] == 361:
        row[''SAMP_PORT_GRP_PV''] = ''A361''
    elif row[''PORTROUTE''] == 371:
        row[''SAMP_PORT_GRP_PV''] = ''A371''
    elif row[''PORTROUTE''] in (381, 382):
        row[''SAMP_PORT_GRP_PV''] = ''A381''
    elif row[''PORTROUTE''] in (341, 391, 393):
        row[''SAMP_PORT_GRP_PV''] = ''A391''
    elif row[''PORTROUTE''] == 401:
        row[''SAMP_PORT_GRP_PV''] = ''A401''
    elif row[''PORTROUTE''] == 411:
        row[''SAMP_PORT_GRP_PV''] = ''A411''
    elif row[''PORTROUTE''] in (421, 423):
        row[''SAMP_PORT_GRP_PV''] = ''A421''
    elif row[''PORTROUTE''] in (441, 443):
        row[''SAMP_PORT_GRP_PV''] = ''A441''
    elif row[''PORTROUTE''] == 451:
        row[''SAMP_PORT_GRP_PV''] = ''A451''
    elif row[''PORTROUTE''] == 461:
        row[''SAMP_PORT_GRP_PV''] = ''A461''
    elif row[''PORTROUTE''] == 471:
        row[''SAMP_PORT_GRP_PV''] = ''A471''
    elif row[''PORTROUTE''] == 481:
        row[''SAMP_PORT_GRP_PV''] = ''A481''
    elif row[''PORTROUTE''] in (611, 612, 613):
        row[''SAMP_PORT_GRP_PV''] = ''DCF''
    elif row[''PORTROUTE''] in (621, 631, 632, 633, 634, 651, 652, 662):
        row[''SAMP_PORT_GRP_PV''] = ''SCF''
    elif row[''PORTROUTE''] == 641:
        row[''SAMP_PORT_GRP_PV''] = ''LHS''
    elif row[''PORTROUTE''] in (635, 636, 661):
        row[''SAMP_PORT_GRP_PV''] = ''SLR''
    elif row[''PORTROUTE''] == 671:
        row[''SAMP_PORT_GRP_PV''] = ''HBN''
    elif row[''PORTROUTE''] == 672:
        row[''SAMP_PORT_GRP_PV''] = ''HGS''
    elif row[''PORTROUTE''] == 681:
        row[''SAMP_PORT_GRP_PV''] = ''EGS''
    elif row[''PORTROUTE''] in (701, 711, 741):
        row[''SAMP_PORT_GRP_PV''] = ''SSE''
    elif row[''PORTROUTE''] in (721, 722):
        row[''SAMP_PORT_GRP_PV''] = ''SNE''
    elif row[''PORTROUTE''] in (731, 682, 691, 692):
        row[''SAMP_PORT_GRP_PV''] = ''RSS''
    elif row[''PORTROUTE''] in (811, 813):
        row[''SAMP_PORT_GRP_PV''] = ''T811''
    elif row[''PORTROUTE''] == 812:
        row[''SAMP_PORT_GRP_PV''] = ''T811''
    elif row[''PORTROUTE''] in (911, 913):
        row[''SAMP_PORT_GRP_PV''] = ''E911''
    elif row[''PORTROUTE''] == 921:
        row[''SAMP_PORT_GRP_PV''] = ''E921''
    elif row[''PORTROUTE''] == 951:
        row[''SAMP_PORT_GRP_PV''] = ''E951''

    Irish = 0
    IoM = 0
    ChannelI = 0
    dvpc = 0

    if dataset == ''survey'':
        if not math.isnan(row[''DVPORTCODE'']):
            dvpc = int(row[''DVPORTCODE''] / 1000)

        if dvpc == 372:
            Irish = 1
        elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if ((row[''FLOW''] in (1, 3)) and (row[''RESIDENCE''] == 372)):
                Irish = 1
            elif ((row[''FLOW''] in (2, 4)) and (row[''COUNTRYVISIT''] == 372)):
                Irish = 1

        if dvpc == 833:
            IoM = 1
        elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if ((row[''FLOW''] in (1, 3)) and (row[''RESIDENCE''] == 833)):
                IoM = 1
            elif ((row[''FLOW''] in (2, 4)) and (row[''COUNTRYVISIT''] == 833)):
                IoM = 1

        if dvpc in (831, 832, 931):
            ChannelI = 1
        elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if ((row[''FLOW''] in (1, 3)) and (row[''RESIDENCE''] in (831, 832, 931))):
                ChannelI = 1
            elif ((row[''FLOW''] in (2, 4)) and (row[''COUNTRYVISIT''] in (831, 832, 931))):
                ChannelI = 1
    elif dataset == ''traffic'':
        if row[''HAUL''] == ''E'':
            Irish = 1
        elif (row[''PORTROUTE''] == 250) or (row[''PORTROUTE''] == 350):
            ChannelI = 1
        elif (row[''PORTROUTE''] == 260) or (row[''PORTROUTE''] == 360):
            IoM = 1

    if (Irish) and row[''PORTROUTE''] in (
    111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152, 171, 173, 174, 175):
        row[''SAMP_PORT_GRP_PV''] = ''AHE''
    elif (Irish) and row[''PORTROUTE''] in (181, 191, 192, 189, 199):
        row[''SAMP_PORT_GRP_PV''] = ''AGE''
    elif (Irish) and row[''PORTROUTE''] in (211, 221, 231, 219):
        row[''SAMP_PORT_GRP_PV''] = ''AME''
    elif (Irish) and row[''PORTROUTE''] in (241, 249):
        row[''SAMP_PORT_GRP_PV''] = ''ALE''
    elif (Irish) and row[''PORTROUTE''] in (201, 202):
        row[''SAMP_PORT_GRP_PV''] = ''ASE''
    elif (Irish) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''SAMP_PORT_GRP_PV''] = ''ARE''
    elif (ChannelI) and (row[''PORTROUTE''] >= 100) and (row[''PORTROUTE''] < 300):
        row[''SAMP_PORT_GRP_PV''] = ''MAC''
    elif (ChannelI) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''SAMP_PORT_GRP_PV''] = ''RAC''
    elif (IoM) and (row[''PORTROUTE''] >= 100) and (row[''PORTROUTE''] < 300):
        row[''SAMP_PORT_GRP_PV''] = ''MAM''
    elif (IoM) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''SAMP_PORT_GRP_PV''] = ''RAM''

    if row[''SAMP_PORT_GRP_PV''] == ''HGS'':
        row[''SAMP_PORT_GRP_PV''] = ''HBN''

    if row[''SAMP_PORT_GRP_PV''] == ''EGS'':
        row[''SAMP_PORT_GRP_PV''] = ''HBN''

    if row[''SAMP_PORT_GRP_PV''] == ''MAM'':
        row[''SAMP_PORT_GRP_PV''] = ''MAC''

    if row[''SAMP_PORT_GRP_PV''] == ''RAM'':
        row[''SAMP_PORT_GRP_PV''] = ''RAC''

    if row[''SAMP_PORT_GRP_PV''] == ''A331'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''A391''
    if row[''SAMP_PORT_GRP_PV''] == ''A331'' and (row[''ARRIVEDEPART''] == 2):
        row[''SAMP_PORT_GRP_PV''] = ''A391''

    if row[''SAMP_PORT_GRP_PV''] == ''A401'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''A441''
    if row[''SAMP_PORT_GRP_PV''] == ''A401'' and (row[''ARRIVEDEPART''] == 2):
        row[''SAMP_PORT_GRP_PV''] = ''A441''

    if row[''SAMP_PORT_GRP_PV''] == ''SLR'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''SCF''
    if row[''SAMP_PORT_GRP_PV''] == ''SLR'' and (row[''ARRIVEDEPART''] == 2):
        row[''SAMP_PORT_GRP_PV''] = ''SCF''

    if row[''SAMP_PORT_GRP_PV''] == ''SSE'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''SNE''

    if row[''SAMP_PORT_GRP_PV''] == ''LHS'':
        row[''SAMP_PORT_GRP_PV''] = ''HBN''
    '
    """

    sql = f"""
    UPDATE PROCESS_VARIABLE_PY
    SET PV_DEF = {samp_port_grp_pv}
    WHERE PV_NAME = 'SAMP_PORT_GRP_PV'
    AND RUN_ID = '{run_id}'
    """

    execute_sql(sql)


if __name__ == '__main__':
    setup_pv()
