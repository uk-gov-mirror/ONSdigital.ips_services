CREATE DATABASE IF NOT EXISTS ips;

grant all on ips.* to 'ips'@'%' with grant option;

use ips;

-- create table AUDIT_LOG
-- (
--     AUDIT_ID          decimal       not null,
--     ACTIONED_BY       varchar(20)   not null,
--     ACTION            varchar(30)   not null,
--     OBJECT            varchar(100)  not null,
--     LOG_DATE          date          not null,
--     AUDIT_LOG_DETAILS varchar(1000) not null
-- );


-- create table COLUMN_LOOKUP
-- (
--     LOOKUP_COLUMN varchar(50)  not null,
--     LOOKUP_KEY    decimal(2)   not null,
--     DISPLAY_VALUE varchar(100) not null
-- );


-- create table DATA_SOURCE
-- (
--     DATA_SOURCE_ID   decimal     not null
--         primary key,
--     DATA_SOURCE_NAME varchar(30) not null
-- );
--
-- INSERT INTO ips.DATA_SOURCE (DATA_SOURCE_ID, DATA_SOURCE_NAME)
-- VALUES (1, 'Sea');
-- INSERT INTO ips.DATA_SOURCE (DATA_SOURCE_ID, DATA_SOURCE_NAME)
-- VALUES (2, 'Air');
-- INSERT INTO ips.DATA_SOURCE (DATA_SOURCE_ID, DATA_SOURCE_NAME)
-- VALUES (3, 'Tunnel');
-- INSERT INTO ips.DATA_SOURCE (DATA_SOURCE_ID, DATA_SOURCE_NAME)
-- VALUES (4, 'Shift');
-- INSERT INTO ips.DATA_SOURCE (DATA_SOURCE_ID, DATA_SOURCE_NAME)
-- VALUES (5, 'Non Response');
-- INSERT INTO ips.DATA_SOURCE (DATA_SOURCE_ID, DATA_SOURCE_NAME)
-- VALUES (6, 'Unsampled');
-- create table DELTAS
-- (
--     DELTA_NUMBER decimal(38)  not null,
--     RUN_DATE     date         not null,
--     BACKOUT_DATE date         null,
--     DESCRIPTION  varchar(100) not null
-- );


-- create table EXPORT_COLUMN
-- (
--     EXPORT_TYPE_ID  decimal     not null,
--     COLUMN_SOURCE   varchar(3)  not null,
--     COLUMN_ORDER_NO decimal(4)  not null,
--     COLUMN_DESC     varchar(30) not null,
--     COLUMN_TYPE     varchar(20) null,
--     COLUMN_LENGTH   decimal(38) null
-- );


-- create table EXPORT_DATA_DETAILS
-- (
--     ED_ID          varchar(40) not null,
--     ED_NAME        varchar(30) not null,
--     EXPORT_TYPE_ID decimal     not null,
--     FORMAT_ID      decimal     not null,
--     DATE_CREATED   date        not null,
--     ED_STATUS      decimal(2)  not null,
--     USER_ID        varchar(20) not null
-- );


create table EXPORT_DATA_DOWNLOAD
(
    RUN_ID            varchar(40) not null,
    DOWNLOADABLE_DATA longtext    null,
    FILENAME          varchar(40) null,
    SOURCE_TABLE      varchar(40) null,
    DATE_CREATED      text        null
);


-- create table EXPORT_TYPE
-- (
--     EXPORT_TYPE_ID   decimal     not null,
--     EXPORT_TYPE_NAME varchar(30) not null,
--     EXPORT_TYPE_DEF  text        not null
-- );


-- create table FORMAT_TYPE
-- (
--     FORMAT_ID   decimal       not null,
--     FORMAT_NAME varchar(30)   not null,
--     FORMAT_DEF  varchar(2000) not null
-- );


create table NON_RESPONSE_DATA
(
    RUN_ID         varchar(40) not null,
    YEAR           decimal(4)  not null,
    MONTH          decimal(2)  not null,
    DATA_SOURCE_ID decimal     not null,
    PORTROUTE      decimal(4)  not null,
    WEEKDAY        decimal(1)  null,
    ARRIVEDEPART   decimal(1)  null,
    AM_PM_NIGHT    decimal(1)  null,
    SAMPINTERVAL   decimal(4)  null,
    MIGTOTAL       decimal     null,
    ORDTOTAL       decimal     null
);


-- create table PROCESS_NAME
-- (
--     PN_ID        decimal     not null,
--     PROCESS_NAME varchar(30) not null
-- );


-- create table PROCESS_VARIABLE
-- (
--     RUN_ID              varchar(40)   not null,
--     PROCESS_VARIABLE_ID decimal       not null,
--     PV_NAME             varchar(30)   not null,
--     PV_DESC             varchar(1000) not null,
--     PV_DEF              text          not null
-- );


-- create table PROCESS_VARIABLE_BACKUP
-- (
--     RUN_ID              varchar(40)   not null,
--     PROCESS_VARIABLE_ID decimal       not null,
--     PV_NAME             varchar(30)   not null,
--     PV_DESC             varchar(1000) not null,
--     PV_DEF              text          not null
-- );


-- create table PROCESS_VARIABLE_LOG
-- (
--     PROCESS_VARIABLE_ID decimal       not null,
--     PVL_DATE            date          not null,
--     PVL_REASON          varchar(1000) not null
-- );


create table PROCESS_VARIABLE_PY
(
    RUN_ID              varchar(40)   not null,
    PROCESS_VARIABLE_ID decimal       not null,
    PV_NAME             varchar(30)   not null,
    PV_DESC             varchar(1000) not null,
    PV_DEF              text          not null
);

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 12, 'imbal_port_fact_pv', 'imbal_port_fact_pv', '
if row[''IMBAL_PORT_GRP_PV''] == 1 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 1.00
elif row[''IMBAL_PORT_GRP_PV''] == 1 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 0.99
elif row[''IMBAL_PORT_GRP_PV''] == 2 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 1.0
elif row[''IMBAL_PORT_GRP_PV''] == 2 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 0.99
elif row[''IMBAL_PORT_GRP_PV''] == 3 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 1.00
elif row[''IMBAL_PORT_GRP_PV''] == 3 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.00
elif row[''IMBAL_PORT_GRP_PV''] == 4 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 1.0
elif row[''IMBAL_PORT_GRP_PV''] == 4 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.0
elif row[''IMBAL_PORT_GRP_PV''] == 5 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 1.0
elif row[''IMBAL_PORT_GRP_PV''] == 5 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.0
elif row[''IMBAL_PORT_GRP_PV''] == 6 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.99
elif row[''IMBAL_PORT_GRP_PV''] == 6 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.04
elif row[''IMBAL_PORT_GRP_PV''] == 7 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.98
elif row[''IMBAL_PORT_GRP_PV''] == 7 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.03
elif row[''IMBAL_PORT_GRP_PV''] == 8 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.96
elif row[''IMBAL_PORT_GRP_PV''] == 8 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.04
elif row[''IMBAL_PORT_GRP_PV''] == 9 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.97
elif row[''IMBAL_PORT_GRP_PV''] == 9 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.04
elif row[''IMBAL_PORT_GRP_PV''] == 10 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.98
elif row[''IMBAL_PORT_GRP_PV''] == 10 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.0
elif row[''IMBAL_PORT_GRP_PV''] == 11 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.96
elif row[''IMBAL_PORT_GRP_PV''] == 11 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.05
elif row[''IMBAL_PORT_GRP_PV''] == 12 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.94
elif row[''IMBAL_PORT_GRP_PV''] == 12 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.06
elif row[''IMBAL_PORT_GRP_PV''] == 13 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.97
elif row[''IMBAL_PORT_GRP_PV''] == 13 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.03
elif row[''IMBAL_PORT_GRP_PV''] == 14 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.97
elif row[''IMBAL_PORT_GRP_PV''] == 14 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.03
elif row[''IMBAL_PORT_GRP_PV''] == 15 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.95
elif row[''IMBAL_PORT_GRP_PV''] == 15 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.03
elif row[''IMBAL_PORT_GRP_PV''] == 16 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.95
elif row[''IMBAL_PORT_GRP_PV''] == 16 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.03
elif row[''IMBAL_PORT_GRP_PV''] == 17 and row[''ARRIVEDEPART''] == 1:
    row[''IMBAL_PORT_FACT_PV''] = 0.97
elif row[''IMBAL_PORT_GRP_PV''] == 17 and row[''ARRIVEDEPART''] == 2:
    row[''IMBAL_PORT_FACT_PV''] = 1.03
else:
    row[''IMBAL_PORT_FACT_PV''] = 1.0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 13, 'stay_imp_flag_pv', 'stay_imp_flag_pv', '
if math.isnan(row[''NUMNIGHTS'']) or row[''NUMNIGHTS''] == 999:
    row[''STAY_IMP_FLAG_PV''] = 1
else:
    row[''STAY_IMP_FLAG_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 14, 'stay_imp_eligible_pv', 'stay_imp_eligible_pv', '
if row[''FLOW''] in (1,4,5,8) and row[''MINS_FLAG_PV''] == 0 and row[''PURPOSE''] != 80:
    row[''STAY_IMP_ELIGIBLE_PV''] = 1
else:
    row[''STAY_IMP_ELIGIBLE_PV''] = 0');

# TODO: Check Me

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 33, 'osport2_pv', 'osport2_pv', '
if row[''UKPORT1_PV''] == 641:
    if not math.isnan(row[''OSPORT1_PV'']):
        row[''OSPORT2_PV''] = int(float(row[''OSPORT1_PV'']) / 100.0)
else:
    if not math.isnan(row[''OSPORT1_PV'']):
        row[''OSPORT2_PV''] = int(float(row[''OSPORT1_PV'']) / 1000.0)
if row[''UKFOREIGN''] == 1 and math.isnan(row[''OSPORT1_PV'']):
    row[''OSPORT2_PV''] = row[''COUNTRYVISIT'']
if row[''UKFOREIGN''] == 2 and math.isnan(row[''OSPORT1_PV'']):
    row[''OSPORT2_PV''] = row[''RESIDENCE'']
if row[''OSPORT2_PV''] == 300:
    row[''OSPORT2_PV''] = 2500
elif row[''OSPORT2_PV''] == 310:
    row[''OSPORT2_PV''] = 2200
elif row[''OSPORT2_PV''] == 320:
    row[''OSPORT2_PV''] = 1000
elif row[''OSPORT2_PV''] == 150:
    row[''OSPORT2_PV''] = 210
elif row[''OSPORT2_PV''] == 160:
    row[''OSPORT2_PV''] = 210
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 47, 'rail_cntry_grp_pv', 'rail_cntry_grp_pv', '
railcountry = 0
if row[''FLOW''] == 5:
    railcountry = row[''RESIDENCE'']
elif row[''FLOW''] == 8:
    railcountry = row[''COUNTRYVISIT'']
if (railcountry == 250):
    row[''RAIL_CNTRY_GRP_PV''] = 1
elif railcountry in (208,578,752):
    row[''RAIL_CNTRY_GRP_PV''] = 2
elif railcountry == 56:
    row[''RAIL_CNTRY_GRP_PV''] = 3
elif railcountry == 276:
    row[''RAIL_CNTRY_GRP_PV''] = 4
elif railcountry == 380:
    row[''RAIL_CNTRY_GRP_PV''] = 5
elif railcountry in (911,912):
    row[''RAIL_CNTRY_GRP_PV''] = 6
elif railcountry == 756:
    row[''RAIL_CNTRY_GRP_PV''] = 7
elif railcountry in (40,442,528,620):
    row[''RAIL_CNTRY_GRP_PV''] = 8
elif railcountry == 372:
    row[''RAIL_CNTRY_GRP_PV''] = 9
elif railcountry == 840:
    row[''RAIL_CNTRY_GRP_PV''] = 10
elif railcountry == 124:
    row[''RAIL_CNTRY_GRP_PV''] = 11
elif railcountry in (112,100,191,203,246,300,348,973,428,440,807,504,616,642,643,703,705,792,804,233):
    row[''RAIL_CNTRY_GRP_PV''] = 12');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 34, 'osport3_pv', 'osport3_pv', '
if row[''OSPORT2_PV''] in (40,56,250,276,372,438,442,492,528,756,830,831,832,833,921,922,923,924,926,931):
    row[''OSPORT3_PV''] = 1
elif row[''OSPORT2_PV''] in (208,233,234,246,248,352,428,440,578,744,752):
    row[''OSPORT3_PV''] = 2
elif row[''OSPORT2_PV''] in (31,51,112,203,268,348,498,616,642,643,703,804):
    row[''OSPORT3_PV''] = 3
elif row[''OSPORT2_PV''] in (8,20,70,100,191,292,300,336,380,470,499,620,621,674,688,705,792,807,901,902,911,912,951,973):
    row[''OSPORT3_PV''] = 4
elif row[''OSPORT2_PV''] in (12,434,504,732,736,788,818):
    row[''OSPORT3_PV''] = 5
elif row[''OSPORT2_PV''] in (24,72,108,120,132,140,148,174,175,178,180,204,226,231,232,262,266,270,288,324,384,404,426,430,450,454,466,478,480,508,516,562,566,624,638,646,654,678,686,690,694,706,710,716,748,768,800,834,854,894):
    row[''OSPORT3_PV''] = 6
elif row[''OSPORT2_PV''] in (60,124,304,840):
    row[''OSPORT3_PV''] = 7
elif row[''OSPORT2_PV''] in (28,32,44,52,68,76,84,92,136,152,170,188,192,212,214,218,222,238,254,308,312,320,328,332,340,388,474,484,500,530,533,558,591,600,604,630,652,659,660,662,663,666,670,740,780,796,850,858,862):
    row[''OSPORT3_PV''] = 8
elif row[''OSPORT2_PV''] in (4,50,64,96,104,116,144,156,158,344,356,360,392,398,408,410,417,418,446,458,462,496,524,586,608,626,702,704,762,764,795,860):
    row[''OSPORT3_PV''] = 9
elif row[''OSPORT2_PV''] in (48,275,364,368,376,400,414,422,512,634,682,760,784,887):
    row[''OSPORT3_PV''] = 10
elif row[''OSPORT2_PV''] in (10,16,36,74,86,90,162,166,184,239,242,258,260,296,316,334,520,540,548,554,570,574,580,581,583,584,585,598,612,772,776,798,876,882):
    row[''OSPORT3_PV''] = 11
elif row[''OSPORT2_PV''] in (940,941,942,943,944,945,946,947,949):
    row[''OSPORT3_PV''] = 12
else:
    row[''OSPORT3_PV''] = 13');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 35, 'osport4_pv', 'osport4_pv', '
if row[''OSPORT3_PV''] in (1,2):
    row[''OSPORT4_PV''] = 1
else:
    row[''OSPORT4_PV''] = 2');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 36, 'apd_pv', 'apd_pv', '
if row[''OSPORT2_PV''] in (210,500,600,700,800):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (1000,1100,1200,1700,2000):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (2100,2200,2300,2390,2500):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (2590,2800,2830,2840,150,160):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (310,320,340,2760,3020,3030):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (3040,3050,3060,3130,3170,3180):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (3000,3010):
    APDBAND = 1
else:
    APDBAND = 2
if row[''FLOW''] > 4:
    row[''APD_PV''] = 0
elif APDBAND == 1:
    row[''APD_PV''] = 10/2
else:
    row[''APD_PV''] = 40/2
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 37, 'qmfare_pv', 'qmfare_pv', '
if row[''OSPORT3_PV''] == 12 and (row[''MINS_FLAG_PV''] == 0 or math.isnan(row[''MINS_FLAG_PV''])):
    row[''QMFARE_PV''] = 1500
else:
    row[''QMFARE_PV''] = None');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 38, 'duty_free_pv', 'duty_free_pv', '
if row[''FLOW''] == 1 and ((row[''PURPOSE''] < 80 and row[''PURPOSE''] != 71) or math.isnan(row[''PURPOSE''])):
    row[''DUTY_FREE_PV''] = 15
else:
    row[''DUTY_FREE_PV''] = 0
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 39, 'spend_imp_eligible_pv', 'spend_imp_eligible_pv', '
if (row[''FLOW''] in (1,4,5,8) and row[''PURPOSE''] < 80 and row[''PURPOSE''] != 23 and row[''PURPOSE''] != 24 and row[''MINS_FLAG_PV''] == 0) or (row[''FLOW''] in (1,4,5,8) and str(row[''PURPOSE'']) == ''nan'' and row[''MINS_FLAG_PV''] == 0):
    row[''SPEND_IMP_ELIGIBLE_PV''] = 1
else:
    row[''SPEND_IMP_ELIGIBLE_PV''] = 0
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 40, 'uk_os_pv', 'uk_os_pv', '
if row[''FLOW''] in (1,5):
    row[''UK_OS_PV''] = 2
if row[''FLOW''] in (4,8):
    row[''UK_OS_PV''] = 1');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 41, 'pur1_pv', 'pur1_pv', '
if row[''DVPACKAGE''] in (1,2):
    row[''IND''] = 1
if row[''DVPACKAGE''] == 9 or math.isnan(row[''DVPACKAGE'']):
    row[''IND''] = 0
if row[''PURPOSE''] in (10,14,17,18):
    row[''PUR1_PV''] = 2
elif row[''PURPOSE''] in (20,21,22):
    row[''PUR1_PV''] = 3
elif row[''PURPOSE''] in (11,12):
    row[''PUR1_PV''] = 4
elif row[''PURPOSE''] == 40:
    row[''PUR1_PV''] = 5
elif row[''PURPOSE''] == 71:
    row[''PUR1_PV''] = 6
else:
    row[''PUR1_PV''] = 7
if row[''IND''] == 1 and row[''PUR1_PV''] == 2:
    row[''PUR1_PV''] = 1
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 42, 'pur2_pv', 'pur2_pv', '
if row[''PURPOSE''] in (10,14,17,18,11,12):
    row[''PUR2_PV''] = 2
elif row[''PURPOSE''] in (20,21,22):
    row[''PUR2_PV''] = 3
elif row[''PURPOSE''] == 71:
    row[''PUR2_PV''] = 4
elif math.isnan(row[''PURPOSE'']):
    row[''PUR2_PV''] = None
else:
    row[''PUR2_PV''] = 5
if row[''IND''] == 1 and row[''PUR2_PV''] == 2:
    row[''PUR2_PV''] = 1');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 43, 'pur3_pv', 'pur3_pv', '
if row[''PURPOSE''] in (20,21,22):
    row[''PUR3_PV''] = 1
elif math.isnan(row[''PURPOSE'']):
    row[''PUR3_PV''] = None
else:
    row[''PUR3_PV''] = 2');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 44, 'dur1_pv', 'dur1_pv', '
if row[''STAY''] == 0:
    row[''DUR1_PV''] = 0
elif row[''STAY''] >= 1 and row[''STAY''] <= 7:
    row[''DUR1_PV''] = 1
elif row[''STAY''] >= 8 and row[''STAY''] <= 21:
    row[''DUR1_PV''] = 2
elif row[''STAY''] >= 22 and row[''STAY''] <= 35:
    row[''DUR1_PV''] = 3
elif row[''STAY''] >= 36 and row[''STAY''] <= 91:
    row[''DUR1_PV''] = 4
elif row[''STAY''] >= 92:
    row[''DUR1_PV''] = 5
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 45, 'dur2_pv', 'dur2_pv', '
if row[''STAY''] >= 0 and row[''STAY''] <= 30:
    row[''DUR2_PV''] = 1
elif row[''STAY''] >= 31:
    row[''DUR2_PV''] = 2
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 46, 'imbal_ctry_fact_pv', 'imbal_ctry_fact_pv', '
if row[''RESIDENCE''] == 352 or row[''RESIDENCE''] == 40 or row[''RESIDENCE''] in (292, 470, 902, 901) or row[''RESIDENCE''] == 792 or row[''RESIDENCE''] == 620 or row[''RESIDENCE''] == 621 or row[''RESIDENCE''] in (973, 70, 191, 807, 499, 688, 951, 705)  or row[''RESIDENCE''] == 234:
    row[''IMBAL_CTRY_FACT_PV''] = 1.02
elif row[''RESIDENCE''] == 56 or row[''RESIDENCE''] == 442:
    row[''IMBAL_CTRY_FACT_PV''] = 0.9
elif (row[''RESIDENCE''] == 250) or row[''RESIDENCE''] == 492:
    row[''IMBAL_CTRY_FACT_PV''] = 1.12
elif row[''RESIDENCE''] == 276:
    row[''IMBAL_CTRY_FACT_PV''] = 0.9
elif (row[''RESIDENCE''] == 380) or row[''RESIDENCE''] == 674:
    row[''IMBAL_CTRY_FACT_PV''] = 0.9
elif row[''RESIDENCE''] == 528:
    row[''IMBAL_CTRY_FACT_PV''] = 0.98
elif row[''RESIDENCE''] == 208:
    row[''IMBAL_CTRY_FACT_PV''] = 1.08
elif row[''RESIDENCE''] in (246, 248, 578, 744, 752):
    row[''IMBAL_CTRY_FACT_PV''] = 0.86
elif (row[''RESIDENCE''] == 300):
    row[''IMBAL_CTRY_FACT_PV''] = 1.06
elif (row[''RESIDENCE''] == 911) or row[''RESIDENCE''] == 20 or row[''RESIDENCE''] == 732:
    row[''IMBAL_CTRY_FACT_PV''] = 1.16
elif row[''RESIDENCE''] == 756 or row[''RESIDENCE''] == 438:
    row[''IMBAL_CTRY_FACT_PV''] = 1.04
elif row[''RESIDENCE''] in (100, 642, 203, 703, 348, 616, 8, 643, 51, 31, 112, 233, 268, 398, 417, 428, 440, 498, 762, 795, 804, 860):
    row[''IMBAL_CTRY_FACT_PV''] = 1.14
elif row[''RESIDENCE''] in (12, 434, 504, 736, 788, 818) or row[''RESIDENCE''] in (48, 400, 414, 512, 634, 784, 275, 376, 364, 368, 422, 682, 887, 760):
    row[''IMBAL_CTRY_FACT_PV''] = 1.1
elif row[''RESIDENCE''] in (270, 288, 566, 694, 654, 404, 426, 454, 480, 690, 834, 800, 894, 72, 748) or row[''RESIDENCE''] in (204, 266, 324, 624, 384, 430, 466, 478, 562, 686, 768, 854, 24, 108, 120, 140, 148, 178, 180, 231, 450, 450, 508, 646, 706, 262, 10, 132, 174, 175, 226, 226, 260, 638, 678, 232) or row[''RESIDENCE''] in (156, 408, 496):
    row[''IMBAL_CTRY_FACT_PV''] = 1.0
elif row[''RESIDENCE''] in (710, 516):
    row[''IMBAL_CTRY_FACT_PV''] = 0.96
elif row[''RESIDENCE''] in (36, 334, 554):
    row[''IMBAL_CTRY_FACT_PV''] = 1.0
elif row[''RESIDENCE''] in (242, 598, 598, 258, 16, 296, 316, 580, 581, 583, 584, 772, 540, 74, 86, 90, 162, 166, 184, 296, 520, 548, 570, 574, 585, 612, 776, 798, 876, 882) or row[''RESIDENCE''] in (50, 96, 144, 458, 702) or (row[''RESIDENCE''] == 356) or row[''RESIDENCE''] in (586, 4, 64, 104, 116, 360, 410, 418, 446, 524, 608, 158, 764, 704, 626):
    row[''IMBAL_CTRY_FACT_PV''] = 1.1
elif row[''RESIDENCE''] == 344:
    row[''IMBAL_CTRY_FACT_PV''] = 1.02
elif (row[''RESIDENCE''] == 392):
    row[''IMBAL_CTRY_FACT_PV''] = 1.16
elif row[''RESIDENCE''] in (60, 388, 780, 28, 44, 52, 92, 136, 212, 308, 500, 659, 660, 662, 670, 796) or row[''RESIDENCE''] in (84, 328, 238, 239):
    row[''IMBAL_CTRY_FACT_PV''] = 1.02
elif row[''RESIDENCE''] in (192, 214, 312, 652, 663, 332, 474, 530, 533) or row[''RESIDENCE''] in (32, 76, 484, 68, 152, 170, 218, 600, 604, 858, 862, 188, 222, 320, 340, 558, 254, 591, 740):
    row[''IMBAL_CTRY_FACT_PV''] = 1.1
elif row[''RESIDENCE''] == 124 or row[''RESIDENCE''] in (666, 304):
    row[''IMBAL_CTRY_FACT_PV''] = 1.04
elif row[''RESIDENCE''] in (840, 630, 850):
    row[''IMBAL_CTRY_FACT_PV''] = 1.04
else:
    row[''IMBAL_CTRY_FACT_PV''] = 1.0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 15, 'StayImpCtryLevel1_pv', 'StayImpCtryLevel1_pv', '
if row[''UKFOREIGN''] == 1:
    if not math.isnan(row[''COUNTRYVISIT'']):
        row[''STAYIMPCTRYLEVEL1_PV''] = int(row[''COUNTRYVISIT''])
if row[''UKFOREIGN''] == 2:
    if not math.isnan(row[''RESIDENCE'']):
        row[''STAYIMPCTRYLEVEL1_PV''] = int(row[''RESIDENCE''])
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 16, 'StayImpCtryLevel2_pv', 'StayImpCtryLevel2_pv', '
if row[''STAYIMPCTRYLEVEL1_PV''] in (830, 831, 832, 833, 931, 372):
    row[''STAYIMPCTRYLEVEL2_PV''] = 1
elif row[''STAYIMPCTRYLEVEL1_PV''] in (250, 56, 442, 528, 492):
    row[''STAYIMPCTRYLEVEL2_PV''] = 2
elif row[''STAYIMPCTRYLEVEL1_PV''] in (620, 621, 911, 912, 20, 292):
    row[''STAYIMPCTRYLEVEL2_PV''] = 3
elif row[''STAYIMPCTRYLEVEL1_PV''] in (276, 40, 756, 438, 208):
    row[''STAYIMPCTRYLEVEL2_PV''] = 4
elif row[''STAYIMPCTRYLEVEL1_PV''] in (470, 901, 902, 380, 792, 300, 674):
    row[''STAYIMPCTRYLEVEL2_PV''] = 5
elif row[''STAYIMPCTRYLEVEL1_PV''] in (352, 248, 246, 578, 744, 752, 234):
    row[''STAYIMPCTRYLEVEL2_PV''] = 6
elif row[''STAYIMPCTRYLEVEL1_PV''] in (70, 191, 807, 499, 951, 688, 705, 100, 642, 203, 703):
    row[''STAYIMPCTRYLEVEL2_PV''] = 7
elif row[''STAYIMPCTRYLEVEL1_PV''] in (348, 616, 8, 643, 51, 31, 112, 233, 268, 398, 417, 428, 440, 498, 762, 795, 804, 860):
    row[''STAYIMPCTRYLEVEL2_PV''] = 8
elif row[''STAYIMPCTRYLEVEL1_PV''] in (12, 434, 504, 736, 788, 818, 732):
    row[''STAYIMPCTRYLEVEL2_PV''] = 11
elif row[''STAYIMPCTRYLEVEL1_PV''] in (270, 288, 566, 694, 654, 404, 426, 454, 480,690, 834, 800, 894, 72, 716, 204, 266, 324,624, 384, 430, 466, 478, 562, 686, 768, 854, 24, 108, 120, 140, 148, 178, 180, 231, 450, 508, 646, 706, 262, 10, 132, 174, 226, 260, 175, 638, 678, 232):
    row[''STAYIMPCTRYLEVEL2_PV''] = 12
elif row[''STAYIMPCTRYLEVEL1_PV''] in (748, 710, 516):
    row[''STAYIMPCTRYLEVEL2_PV''] = 13
elif row[''STAYIMPCTRYLEVEL1_PV''] in (400, 376, 275, 422, 887):
    row[''STAYIMPCTRYLEVEL2_PV''] = 14
elif row[''STAYIMPCTRYLEVEL1_PV''] in (48, 414, 512, 634, 784, 364, 368, 682, 760):
    row[''STAYIMPCTRYLEVEL2_PV''] = 15
elif row[''STAYIMPCTRYLEVEL1_PV''] in (462, 50, 144, 356, 586):
    row[''STAYIMPCTRYLEVEL2_PV''] = 21
elif row[''STAYIMPCTRYLEVEL1_PV''] in (344, 156, 496, 524):
    row[''STAYIMPCTRYLEVEL2_PV''] = 22
elif row[''STAYIMPCTRYLEVEL1_PV''] in (96, 458, 360, 608):
    row[''STAYIMPCTRYLEVEL2_PV''] = 23
elif row[''STAYIMPCTRYLEVEL1_PV''] in (702, 392, 158, 764):
    row[''STAYIMPCTRYLEVEL2_PV''] = 24
elif row[''STAYIMPCTRYLEVEL1_PV''] in (4, 104, 116, 410, 418, 446, 704, 626, 408):
    row[''STAYIMPCTRYLEVEL2_PV''] = 25
elif row[''STAYIMPCTRYLEVEL1_PV''] in (334, 36, 554):
    row[''STAYIMPCTRYLEVEL2_PV''] = 31
elif row[''STAYIMPCTRYLEVEL1_PV''] in (242, 598, 258, 296, 316, 581, 580, 584, 583, 16, 772, 581, 540, 74, 86, 162, 166, 184, 798, 520, 570, 574, 585, 612, 882, 90, 776, 798, 548, 876):
    row[''STAYIMPCTRYLEVEL2_PV''] = 32
elif row[''STAYIMPCTRYLEVEL1_PV''] in (124, 666, 304):
    row[''STAYIMPCTRYLEVEL2_PV''] = 41
elif row[''STAYIMPCTRYLEVEL1_PV''] in (840, 630, 850):
    row[''STAYIMPCTRYLEVEL2_PV''] = 42
elif row[''STAYIMPCTRYLEVEL1_PV''] in (60, 388, 780, 28, 44, 52, 92, 136, 212, 308, 500, 660, 659, 662, 670, 796, 192, 214, 312, 652, 663, 332, 474, 530, 533, 84, 328):
    row[''STAYIMPCTRYLEVEL2_PV''] = 43
elif row[''STAYIMPCTRYLEVEL1_PV''] in (484, 340, 320,222, 558, 188, 862, 591):
    row[''STAYIMPCTRYLEVEL2_PV''] = 44
elif row[''STAYIMPCTRYLEVEL1_PV''] in (76, 68, 152, 170, 218, 604, 858, 862, 254, 740):
    row[''STAYIMPCTRYLEVEL2_PV''] = 45
elif row[''STAYIMPCTRYLEVEL1_PV''] in (238, 32, 858, 600, 152):
    row[''STAYIMPCTRYLEVEL2_PV''] = 46
elif row[''STAYIMPCTRYLEVEL1_PV''] in (40, 42, 43, 44):
    row[''STAYIMPCTRYLEVEL2_PV''] = 51
elif row[''STAYIMPCTRYLEVEL1_PV''] in (41,45, 46, 47, 49):
    row[''STAYIMPCTRYLEVEL2_PV''] = 52
elif row[''STAYIMPCTRYLEVEL1_PV''] in (0, 969, 99):
    row[''STAYIMPCTRYLEVEL2_PV''] = 91
else:
    row[''STAYIMPCTRYLEVEL2_PV''] = 99');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 28, 'ukport1_pv', 'ukport1_pv', '
if (row[''PORTROUTE''] >= 111 and row[''PORTROUTE''] <= 119) or row[''PORTROUTE''] in (161,171):
    row[''UKPORT1_PV''] = 110
elif (row[''PORTROUTE''] >= 121 and row[''PORTROUTE''] <= 129) or row[''PORTROUTE''] in (162,172):
    row[''UKPORT1_PV''] = 120
elif (row[''PORTROUTE''] >= 131 and row[''PORTROUTE''] <= 139) or row[''PORTROUTE''] in (163,173):
    row[''UKPORT1_PV''] = 130
elif (row[''PORTROUTE''] >= 141 and row[''PORTROUTE''] <= 149) or row[''PORTROUTE''] in (164,174):
    row[''UKPORT1_PV''] = 140
elif (row[''PORTROUTE''] >= 151 and row[''PORTROUTE''] <= 159) or row[''PORTROUTE''] in (165,175):
    row[''UKPORT1_PV''] = 150
elif row[''PORTROUTE''] >= 181 and row[''PORTROUTE''] <= 189:
    row[''UKPORT1_PV''] = 180
elif row[''PORTROUTE''] >= 191 and row[''PORTROUTE''] <= 199:
    row[''UKPORT1_PV''] = 190
elif row[''PORTROUTE''] >= 201 and row[''PORTROUTE''] <= 209:
    row[''UKPORT1_PV''] = 200
elif row[''PORTROUTE''] >= 211 and row[''PORTROUTE''] <= 219:
    row[''UKPORT1_PV''] = 210
elif row[''PORTROUTE''] >= 221 and row[''PORTROUTE''] <= 229:
    row[''UKPORT1_PV''] = 220
elif row[''PORTROUTE''] >= 241 and row[''PORTROUTE''] <= 249:
    row[''UKPORT1_PV''] = 240
elif row[''PORTROUTE''] >= 311 and row[''PORTROUTE''] <= 319:
    row[''UKPORT1_PV''] = 310
else:
    row[''UKPORT1_PV''] = row[''PORTROUTE'']');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 29, 'ukport2_pv', 'ukport2_pv', '
if row[''UKPORT1_PV''] >= 110 and row[''UKPORT1_PV''] <= 150:
    row[''UKPORT2_PV''] = 1
elif row[''UKPORT1_PV''] in (180,190):
    row[''UKPORT2_PV''] = 2
elif (row[''UKPORT1_PV''] >= 210 and row[''UKPORT1_PV''] <= 231):
    row[''UKPORT2_PV''] = 3
elif row[''UKPORT1_PV''] == 200:
    row[''UKPORT2_PV''] = 4
elif row[''UKPORT1_PV''] == 340:
    row[''UKPORT2_PV''] = 5
elif row[''UKPORT1_PV''] in (381,391,451):
    row[''UKPORT2_PV''] = 10
elif row[''UKPORT1_PV''] in (401,411,441):
    row[''UKPORT2_PV''] = 11
elif row[''UKPORT1_PV''] in (310,371):
    row[''UKPORT2_PV''] = 12
elif row[''UKPORT1_PV''] == 421:
    row[''UKPORT2_PV''] = 13
elif row[''UKPORT1_PV''] in (351,361):
    row[''UKPORT2_PV''] = 14
elif row[''UKPORT1_PV''] in (461,481):
    row[''UKPORT2_PV''] = 15
elif row[''UKPORT1_PV''] in (611,612):
    row[''UKPORT2_PV''] = 21
elif row[''UKPORT1_PV''] in (631,632,633,634) or (row[''UKPORT1_PV''] >= 651 and row[''UKPORT1_PV''] <= 662):
    row[''UKPORT2_PV''] = 22
elif row[''UKPORT1_PV''] in (671, 672):
    row[''UKPORT2_PV''] = 23
elif row[''UKPORT1_PV''] >= 681 and row[''UKPORT1_PV''] <= 692:
    row[''UKPORT2_PV''] = 24
elif row[''UKPORT1_PV''] in (701,711):
    row[''UKPORT2_PV''] = 25
elif row[''UKPORT1_PV''] in (721,722):
    row[''UKPORT2_PV''] = 26
elif row[''UKPORT1_PV''] == 641:
    row[''UKPORT2_PV''] = 27
elif row[''UKPORT1_PV''] in (811,812):
    row[''UKPORT2_PV''] = 31
elif row[''UKPORT1_PV''] >= 911 and row[''UKPORT1_PV''] <= 951:
    row[''UKPORT2_PV''] = 32
else:
    row[''UKPORT2_PV''] = 99');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 30, 'ukport3_pv', 'ukport3_pv', '
if row[''UKPORT2_PV''] in (1,2,4,13):
    row[''UKPORT3_PV''] = 1
elif row[''UKPORT2_PV''] in (3,10,11):
    row[''UKPORT3_PV''] = 2
elif row[''UKPORT2_PV''] in (12,14):
    row[''UKPORT3_PV''] = 3
elif row[''UKPORT2_PV''] in (21,22):
    row[''UKPORT3_PV''] = 4
elif row[''UKPORT2_PV''] in (23,24):
    row[''UKPORT3_PV''] = 5
elif row[''UKPORT2_PV''] in (25,26):
    row[''UKPORT3_PV''] = 6
elif row[''UKPORT2_PV''] == 27:
    row[''UKPORT3_PV''] = 7
elif row[''UKPORT2_PV''] == 31:
    row[''UKPORT3_PV''] = 8
elif row[''UKPORT2_PV''] == 32:
    row[''UKPORT3_PV''] = 9
else:
    row[''UKPORT3_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 31, 'ukport4_pv', 'ukport4_pv', '
if row[''UKPORT3_PV''] in (1,2,3,9):
    row[''UKPORT4_PV''] = 1
elif row[''UKPORT3_PV''] in (4,5,6,7,8):
    row[''UKPORT4_PV''] = 2
else:
    row[''UKPORT4_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 32, 'osport1_pv', 'osport1_pv', '
row[''OSPORT1_PV''] = row[''DVPORTCODE'']
if not math.isnan(row[''CHANGECODE'']):
    row[''OSPORT1_PV''] = row[''CHANGECODE'']
if row[''OSPORT1_PV''] in (999998,999999):
    row[''OSPORT1_PV''] = float(''NaN'')
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 48, 'rail_exercise_pv', 'rail_exercise_pv', '
if row[''FLOW''] == 8:
    if row[''RAIL_CNTRY_GRP_PV''] == 1:
        row[''RAIL_EXERCISE_PV''] = 38
    elif row[''RAIL_CNTRY_GRP_PV''] == 2:
        row[''RAIL_EXERCISE_PV''] = 59
    elif row[''RAIL_CNTRY_GRP_PV''] == 3:
        row[''RAIL_EXERCISE_PV''] = 25
    elif row[''RAIL_CNTRY_GRP_PV''] == 4:
        row[''RAIL_EXERCISE_PV''] = 36
    elif row[''RAIL_CNTRY_GRP_PV''] == 5:
        row[''RAIL_EXERCISE_PV''] = 43
    elif row[''RAIL_CNTRY_GRP_PV''] == 6:
        row[''RAIL_EXERCISE_PV''] = 4
    elif row[''RAIL_CNTRY_GRP_PV''] == 7:
        row[''RAIL_EXERCISE_PV''] = 28
    elif row[''RAIL_CNTRY_GRP_PV''] == 8:
        row[''RAIL_EXERCISE_PV''] = 65
    elif row[''RAIL_CNTRY_GRP_PV''] == 9:
        row[''RAIL_EXERCISE_PV''] = 0
    elif row[''RAIL_CNTRY_GRP_PV''] == 10:
        row[''RAIL_EXERCISE_PV''] = 0
    elif row[''RAIL_CNTRY_GRP_PV''] == 11:
        row[''RAIL_EXERCISE_PV''] = 0
    elif row[''RAIL_CNTRY_GRP_PV''] == 12:
        row[''RAIL_EXERCISE_PV''] = 28
elif row[''FLOW''] == 5:
    if row[''RAIL_CNTRY_GRP_PV''] == 1:
        row[''RAIL_EXERCISE_PV''] = 137
    elif row[''RAIL_CNTRY_GRP_PV''] == 2:
        row[''RAIL_EXERCISE_PV''] = 146
    elif row[''RAIL_CNTRY_GRP_PV''] == 3:
        row[''RAIL_EXERCISE_PV''] = 23
    elif row[''RAIL_CNTRY_GRP_PV''] == 4:
        row[''RAIL_EXERCISE_PV''] = 304
    elif row[''RAIL_CNTRY_GRP_PV''] == 5:
        row[''RAIL_EXERCISE_PV''] = 4
    elif row[''RAIL_CNTRY_GRP_PV''] == 6:
        row[''RAIL_EXERCISE_PV''] = 36
    elif row[''RAIL_CNTRY_GRP_PV''] == 7:
        row[''RAIL_EXERCISE_PV''] = 67
    elif row[''RAIL_CNTRY_GRP_PV''] == 8:
        row[''RAIL_EXERCISE_PV''] = 9
    elif row[''RAIL_CNTRY_GRP_PV''] == 9:
        row[''RAIL_EXERCISE_PV''] = 0
    elif row[''RAIL_CNTRY_GRP_PV''] == 10:
        row[''RAIL_EXERCISE_PV''] = 0
    elif row[''RAIL_CNTRY_GRP_PV''] == 11:
        row[''RAIL_EXERCISE_PV''] = 0
    elif row[''RAIL_CNTRY_GRP_PV''] == 12:
        row[''RAIL_EXERCISE_PV''] = 23
row[''RAIL_EXERCISE_PV''] = row[''RAIL_EXERCISE_PV''] * 1000');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 49, 'rail_imp_eligible_pv', 'rail_imp_eligible_pv', '
if row[''FLOW''] in (5,8):
    row[''RAIL_IMP_ELIGIBLE_PV''] = 1
else:
    row[''RAIL_IMP_ELIGIBLE_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 50, 'spend_imp_flag_pv', 'spend_imp_flag_pv', '
if math.isnan(row[''SPEND'']):
    row[''SPEND_IMP_FLAG_PV''] = 1
else:
    row[''SPEND_IMP_FLAG_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 51, 'purpose_pv', 'purpose_pv', '
if row[''PURPOSE''] == 3 or row[''PURPOSE''] == 4 or row[''PURPOSE''] == 31 or row[''PURPOSE''] == 32:
    row[''PURPOSE_PV''] = 1
elif row[''PURPOSE''] == 1 or row[''PURPOSE''] == 2:
    row[''PURPOSE_PV''] = 2
elif row[''PURPOSE''] == 5:
    row[''PURPOSE_PV''] = 3
elif row[''PURPOSE''] == 61 or row[''PURPOSE''] == 62 or row[''PURPOSE''] == 6:
    row[''PURPOSE_PV''] = 4
else:
    row[''PURPOSE_PV''] = 5');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 52, 'town_imp_eligible_pv', 'town_imp_eligible_pv', '
if row[''FLOW''] in (1,5) and row[''RESPNSE''] != 5 and (row[''PURPOSE''] <= 89 or row[''PURPOSE''] == 92 or math.isnan(row[''PURPOSE''])):
    row[''TOWN_IMP_ELIGIBLE_PV''] = 1
else:
    row[''TOWN_IMP_ELIGIBLE_PV''] = 0
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 53, 'reg_imp_eligible_pv', 'reg_imp_eligible_pv', '
if row[''FLOW''] in (1,5) and row[''RESPNSE''] != 5 and (row[''PURPOSE''] <= 89 or row[''PURPOSE''] == 92 or math.isnan(row[''PURPOSE''])):
    row[''REG_IMP_ELIGIBLE_PV''] = 1
else:
    row[''REG_IMP_ELIGIBLE_PV''] = 0
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 54, 'mins_ctry_grp_pv', 'mins_ctry_grp_pv', '
row[''MINS_CTRY_GRP_PV''] = row[''FLOW'']');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 55, 'mins_port_grp_pv', 'mins_port_grp_pv', '
row[''MINS_PORT_GRP_PV''] = int(row[''PORTROUTE''])');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 56, 'samp_port_grp_pv', 'samp_port_grp_pv', '
if row[''PORTROUTE''] in (111, 113, 119, 161, 171):
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
elif row[''PORTROUTE''] in (635,636,661):
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
        if ((row[''FLOW''] in (1,3)) and (row[''RESIDENCE''] == 372)):
            Irish = 1
        elif ((row[''FLOW''] in (2,4)) and (row[''COUNTRYVISIT''] == 372)):
            Irish = 1
    if dvpc == 833:
        IoM = 1
    elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
        if ((row[''FLOW''] in (1,3)) and (row[''RESIDENCE''] == 833)):
            IoM = 1
        elif ((row[''FLOW''] in (2,4)) and (row[''COUNTRYVISIT''] == 833)):
            IoM = 1
    if dvpc in (831, 832, 931):
        ChannelI = 1
    elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
        if ((row[''FLOW''] in (1,3)) and (row[''RESIDENCE''] in (831, 832, 931))):
            ChannelI = 1
        elif ((row[''FLOW''] in (2,4)) and (row[''COUNTRYVISIT''] in (831, 832, 931))):
            ChannelI = 1
elif dataset == ''traffic'':
    if row[''HAUL''] == ''E'':
        Irish = 1
    elif ( row[''PORTROUTE''] == 250) or ( row[''PORTROUTE''] == 350):
        ChannelI = 1
    elif ( row[''PORTROUTE''] == 260) or (row[''PORTROUTE''] == 360):
        IoM = 1
if (Irish) and row[''PORTROUTE''] in (111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152, 171, 173, 174, 175):
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
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('Q3-TEST', 59, 'samp_port_grp_pv', 'samp_port_grp_pv', '
if row[''PORTROUTE''] in (111, 113, 119, 161, 171):
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
elif row[''PORTROUTE''] in (231, 233, 234):
    row[''SAMP_PORT_GRP_PV''] = ''A231''
elif row[''PORTROUTE''] in (241, 243, 249):
    row[''SAMP_PORT_GRP_PV''] = ''A241''
elif row[''PORTROUTE''] in (311, 321, 313, 319):
    row[''SAMP_PORT_GRP_PV''] = ''A311''
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

if (Irish) and row[''PORTROUTE''] in (111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152, 171, 173, 174, 175):
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

if row[''SAMP_PORT_GRP_PV''] == ''LHS'':
    row[''SAMP_PORT_GRP_PV''] = ''HBN''
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 57, 'unsamp_port_grp_pv', 'unsamp_port_grp_pv', '
if row[''PORTROUTE''] in (111, 113, 119, 161):
    row[''UNSAMP_PORT_GRP_PV''] = ''A111''
elif row[''PORTROUTE''] in (121, 123, 162, 172):
    row[''UNSAMP_PORT_GRP_PV''] = ''A121''
elif row[''PORTROUTE''] in (131, 132, 133, 134, 135, 163, 173):
    row[''UNSAMP_PORT_GRP_PV''] = ''A131''
elif row[''PORTROUTE''] in (141, 142, 143, 144, 145, 164):
    row[''UNSAMP_PORT_GRP_PV''] = ''A141''
elif row[''PORTROUTE''] in (151, 152, 153, 165):
    row[''UNSAMP_PORT_GRP_PV''] = ''A151''
elif row[''PORTROUTE''] in (181, 183, 189):
    row[''UNSAMP_PORT_GRP_PV''] = ''A181''
elif row[''PORTROUTE''] in (191, 192, 193, 199):
    row[''UNSAMP_PORT_GRP_PV''] = ''A191''
elif row[''PORTROUTE''] in (201, 202, 203):
    row[''UNSAMP_PORT_GRP_PV''] = ''A201''
elif row[''PORTROUTE''] in (211, 213, 219):
    row[''UNSAMP_PORT_GRP_PV''] = ''A211''
elif row[''PORTROUTE''] in (221, 223):
    row[''UNSAMP_PORT_GRP_PV''] = ''A221''
elif row[''PORTROUTE''] in (231, 232):
    row[''UNSAMP_PORT_GRP_PV''] = ''A231''
elif row[''PORTROUTE''] in (241, 243):
    row[''UNSAMP_PORT_GRP_PV''] = ''A241''
elif row[''PORTROUTE''] in (381, 382, 391, 341, 331, 451):
    row[''UNSAMP_PORT_GRP_PV''] = ''A991''
elif row[''PORTROUTE''] in (401, 411, 441, 471):
    row[''UNSAMP_PORT_GRP_PV''] = ''A992''
elif row[''PORTROUTE''] in (311, 371, 421, 321, 319):
    row[''UNSAMP_PORT_GRP_PV''] = ''A993''
elif row[''PORTROUTE''] in (461, 351, 361, 481):
    row[''UNSAMP_PORT_GRP_PV''] = ''A994''
elif row[''PORTROUTE''] == 991:
    row[''UNSAMP_PORT_GRP_PV''] = ''A991''
elif row[''PORTROUTE''] == 992:
    row[''UNSAMP_PORT_GRP_PV''] = ''A992''
elif row[''PORTROUTE''] == 993:
    row[''UNSAMP_PORT_GRP_PV''] = ''A993''
elif row[''PORTROUTE''] == 994:
    row[''UNSAMP_PORT_GRP_PV''] = ''A994''
elif row[''PORTROUTE''] == 995:
    row[''UNSAMP_PORT_GRP_PV''] = ''ARE''
elif row[''PORTROUTE''] in (611, 612, 613, 851, 853, 868, 852):
    row[''UNSAMP_PORT_GRP_PV''] = ''DCF''
elif row[''PORTROUTE''] in (621, 631, 632, 633, 634, 854):
    row[''UNSAMP_PORT_GRP_PV''] = ''SCF''
elif row[''PORTROUTE''] in (641, 865):
    row[''UNSAMP_PORT_GRP_PV''] = ''LHS''
elif row[''PORTROUTE''] in (635, 636, 651, 652, 661, 662, 856):
    row[''UNSAMP_PORT_GRP_PV''] = ''SLR''
elif row[''PORTROUTE''] in (671, 859, 860, 855):
    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''
elif row[''PORTROUTE''] in (672, 858):
    row[''UNSAMP_PORT_GRP_PV''] = ''HGS''
elif row[''PORTROUTE''] in (681, 682, 691, 692, 862):
    row[''UNSAMP_PORT_GRP_PV''] = ''EGS''
elif row[''PORTROUTE''] in (701, 711, 741, 864):
    row[''UNSAMP_PORT_GRP_PV''] = ''SSE''
elif row[''PORTROUTE''] in (721, 722, 863):
    row[''UNSAMP_PORT_GRP_PV''] = ''SNE''
elif row[''PORTROUTE''] in (731, 861):
    row[''UNSAMP_PORT_GRP_PV''] = ''RSS''
elif row[''PORTROUTE''] == 811:
    row[''UNSAMP_PORT_GRP_PV''] = ''T811''
elif row[''PORTROUTE''] == 812:
    row[''UNSAMP_PORT_GRP_PV''] = ''T812''
elif row[''PORTROUTE''] == 911:
    row[''UNSAMP_PORT_GRP_PV''] = ''E911''
elif row[''PORTROUTE''] == 921:
    row[''UNSAMP_PORT_GRP_PV''] = ''E921''
elif row[''PORTROUTE''] == 951:
    row[''UNSAMP_PORT_GRP_PV''] = ''E951''
Irish = 0
IoM = 0
ChannelI = 0
dvpc = 0
if dataset == ''survey'':
    if not math.isnan(row[''DVPORTCODE'']):
        dvpc = int(row[''DVPORTCODE''] / 1000)
    if dvpc == 372:
        Irish = 1
    elif (row[''DVPORTCODE''] == 999999) or math.isnan(row[''DVPORTCODE'']):
        if ((row[''FLOW''] in (1,3)) and (row[''RESIDENCE''] == 372)):
            Irish = 1
        elif ((row[''FLOW''] in (2,4)) and (row[''COUNTRYVISIT''] == 372)):
            Irish = 1
    if dvpc == 833:
        IoM = 1
    elif (row[''DVPORTCODE''] == 999999) or math.isnan(row[''DVPORTCODE'']):
        if ((row[''FLOW''] in (1,3)) and (row[''RESIDENCE''] == 833)):
            IoM = 1
        elif ((row[''FLOW''] in (2,4)) and (row[''COUNTRYVISIT''] == 833)):
            IoM = 1
    if dvpc in (831, 832, 931):
        ChannelI = 1
    elif (row[''DVPORTCODE''] == 999999) or math.isnan(row[''DVPORTCODE'']):
        if ((row[''FLOW''] in (1,3)) and (row[''RESIDENCE''] in (831, 832, 931))):
            ChannelI = 1
        elif ((row[''FLOW''] in (2,4)) and (row[''COUNTRYVISIT''] in (831, 832, 931))):
            ChannelI = 1
    if (Irish) and row[''PORTROUTE''] in (111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152):
        row[''UNSAMP_PORT_GRP_PV''] = ''AHE''
    elif (Irish) and row[''PORTROUTE''] in (181, 191, 192, 189, 199):
        row[''UNSAMP_PORT_GRP_PV''] = ''AGE''
    elif (Irish) and row[''PORTROUTE''] in (211, 221, 231, 219, 249):
        row[''UNSAMP_PORT_GRP_PV''] = ''AME''
    elif (Irish) and row[''PORTROUTE''] == 241:
        row[''UNSAMP_PORT_GRP_PV''] = ''ALE''
    elif (Irish) and row[''PORTROUTE''] in (201, 202):
        row[''UNSAMP_PORT_GRP_PV''] = ''ASE''
    elif (Irish) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''UNSAMP_PORT_GRP_PV''] = ''ARE''
    elif (ChannelI) and (row[''PORTROUTE''] >= 100) and (row[''PORTROUTE''] < 300):
        row[''UNSAMP_PORT_GRP_PV''] = ''MAC''
    elif (ChannelI) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''UNSAMP_PORT_GRP_PV''] = ''RAC''
    elif (IoM) and (row[''PORTROUTE''] >= 100) and (row[''PORTROUTE''] < 300):
        row[''UNSAMP_PORT_GRP_PV''] = ''MAM''
    elif (IoM) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''UNSAMP_PORT_GRP_PV''] = ''RAM''
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 4, 'shift_flag_pv', 'shift_flag_pv', '
if row[''PORTROUTE''] < 600 or row[''PORTROUTE''] > 900:
    row[''SHIFT_FLAG_PV''] = 1
else:
    row[''SHIFT_FLAG_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 5, 'crossings_flag_pv', 'crossings_flag_pv', '
if row[''PORTROUTE''] < 600 or row[''PORTROUTE''] > 900:
    row[''CROSSINGS_FLAG_PV''] = 0
else:
    row[''CROSSINGS_FLAG_PV''] = 1');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 6, 'shift_port_grp_pv', 'shift_port_grp_pv', '
if row[''PORTROUTE''] >= 161 and row[''PORTROUTE''] <= 165:
    row[''SHIFT_PORT_GRP_PV''] = ''LHR Transits''
elif row[''PORTROUTE''] >= 171 and row[''PORTROUTE''] <= 175:
    row[''SHIFT_PORT_GRP_PV''] = ''LHR Mig Transits''
else:
    #  row[''SHIFT_PORT_GRP_PV''] = str(row[''PORTROUTE'']).rjust(3,'' '')
    row[''SHIFT_PORT_GRP_PV''] = str(int(row[''PORTROUTE'']))
  ');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 7, 'nr_flag_pv', 'nr_flag_pv', '
if row[''RESPNSE''] > 0 and row[''RESPNSE''] < 4:
    row[''NR_FLAG_PV''] = 0
elif row[''RESPNSE''] >= 4 and row[''RESPNSE''] < 7:
    row[''NR_FLAG_PV''] = 1
else:
    row[''NR_FLAG_PV''] = 2');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 8, 'nr_port_grp_pv', 'nr_port_grp_pv', '
row[''NR_PORT_GRP_PV''] = row[''PORTROUTE'']');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 9, 'mins_flag_pv', 'mins_flag_pv', '
if row[''TYPEINTERVIEW''] == 1:
    row[''MINS_FLAG_PV''] = 2
elif row[''RESPNSE''] == 1 or row[''RESPNSE''] == 2:
    row[''MINS_FLAG_PV''] = 0
elif row[''RESPNSE''] == 3:
    row[''MINS_FLAG_PV''] = 1
else:
    row[''MINS_FLAG_PV''] = 3');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 10, 'imbal_eligible_pv', 'imbal_eligible_pv', '
if not math.isnan(row[''FLOW'']) and (row[''RESPNSE''] > 0) and (row[''RESPNSE''] < 3) and ((row[''PURPOSE''] != 23) and (row[''PURPOSE''] != 24) and (row[''PURPOSE''] < 71 or math.isnan(row[''PURPOSE'']))) and (math.isnan(row[''INTENDLOS'']) or (row[''INTENDLOS''] < 2) or (row[''INTENDLOS''] > 7)):
    row[''IMBAL_ELIGIBLE_PV''] = 1
else:
    row[''IMBAL_ELIGIBLE_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 11, 'imbal_port_grp_pv', 'imbal_port_grp_pv', '
if row[''PORTROUTE''] in (111, 113, 119, 161, 171):
    row[''IMBAL_PORT_GRP_PV''] = 1
elif row[''PORTROUTE''] in (121, 123, 151, 153, 162, 165, 172, 175):
    row[''IMBAL_PORT_GRP_PV''] = 2
elif row[''PORTROUTE''] in (131, 132, 133, 134, 135, 163, 173):
    row[''IMBAL_PORT_GRP_PV''] = 3
elif row[''PORTROUTE''] in (141, 142, 143, 144, 145, 164, 174):
    row[''IMBAL_PORT_GRP_PV''] = 4
elif row[''PORTROUTE''] in (191, 193):
    row[''IMBAL_PORT_GRP_PV''] = 5
elif row[''PORTROUTE''] in (181, 183, 189, 199):
    row[''IMBAL_PORT_GRP_PV''] = 6
elif row[''PORTROUTE''] in (211, 213, 219, 221, 223, 231):
    row[''IMBAL_PORT_GRP_PV''] = 7
elif row[''PORTROUTE''] in (201, 203):
    row[''IMBAL_PORT_GRP_PV''] = 8
elif row[''PORTROUTE''] in (241, 243, 311, 319, 321, 351, 361, 371, 381, 391, 401, 411, 421, 441, 451, 461, 471, 481):
    row[''IMBAL_PORT_GRP_PV''] = 9
elif row[''PORTROUTE''] in (641, 671, 672, 681, 682, 691, 692, 731):
    row[''IMBAL_PORT_GRP_PV''] = 10
elif row[''PORTROUTE''] in (611, 612, 701, 711, 721, 722, 812):
    row[''IMBAL_PORT_GRP_PV''] = 11
elif row[''PORTROUTE''] in (621, 631, 632, 633, 634, 651, 661, 662):
    row[''IMBAL_PORT_GRP_PV''] = 12
elif row[''PORTROUTE''] == 911:
    row[''IMBAL_PORT_GRP_PV''] = 13
elif row[''PORTROUTE''] == 921:
    row[''IMBAL_PORT_GRP_PV''] = 14
elif row[''PORTROUTE''] == 811:
    row[''IMBAL_PORT_GRP_PV''] = 15
elif row[''IMBAL_PORT_GRP_PV''] == 9999:
    row[''IMBAL_PORT_GRP_PV''] = 16
elif row[''PORTROUTE''] == 951:
    row[''IMBAL_PORT_GRP_PV''] = 17');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 58, 'unsamp_region_grp_pv', 'unsamp_region_grp_pv', '
dvpc = 0
row[''ARRIVEDEPART''] = int(row[''ARRIVEDEPART''])
if dataset == ''survey'':
    if not math.isnan(row[''DVPORTCODE'']):
        dvpc = int(row[''DVPORTCODE''] / 1000)
    if row[''PORTROUTE''] < 300:
        if row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if row[''FLOW''] in (1,3):
                row[''REGION''] = row[''RESIDENCE'']
            elif row[''FLOW''] in (2,4):
                row[''REGION''] = row[''COUNTRYVISIT'']
            else:
                row[''REGION''] = ''''
        else:
            row[''REGION''] = dvpc
        if row[''REGION''] in (8, 20, 31, 40, 51, 56, 70, 100, 112, 191, 203, 208, 233, 234, 246, 250, 268, 276, 348, 352, 380, 398, 417, 428, 440, 442, 492, 498, 499, 528, 578, 616, 642, 643, 674, 688, 703, 705, 752, 756, 762, 795, 804, 807, 860, 940, 942, 943, 944, 945, 946, 950, 951):
            row[''UNSAMP_REGION_GRP_PV''] = 1.0
        elif row[''REGION''] in (124, 304, 630, 666, 840, 850):
            row[''UNSAMP_REGION_GRP_PV''] = 2.0
        elif row[''REGION''] in (4, 36, 50, 64, 96, 104, 116, 126, 144, 156, 158, 242, 356, 360, 408, 410, 418, 446, 458, 462, 496, 524, 554, 586, 608, 626, 702, 704, 764):
            row[''UNSAMP_REGION_GRP_PV''] = 3.0
        elif row[''REGION''] in (12, 24, 48, 72, 108, 120, 132, 140, 148, 174, 178, 180, 204, 226, 231, 232, 262, 266, 270, 288, 324, 348, 384, 404, 426, 430, 434, 450, 454, 466, 478, 480, 504, 508, 516, 562, 566, 624, 646, 654, 678, 686, 690, 694, 706, 710, 716, 732, 736, 748, 768, 788, 800, 818, 834, 854, 894):
            row[''UNSAMP_REGION_GRP_PV''] = 4.0
        elif row[''REGION''] == 392:
            row[''UNSAMP_REGION_GRP_PV''] = 5.0
        elif row[''REGION''] == 344:
            row[''UNSAMP_REGION_GRP_PV''] = 6.0
        elif row[''REGION''] in (16, 28, 32, 44, 48, 52, 60, 68, 76, 84, 90, 92, 136, 152, 166, 170, 184, 188, 192, 212, 214, 218, 222, 238, 254, 258, 296, 308, 312, 316, 320, 328, 332, 340, 364, 368, 376, 388, 400, 414, 422, 474, 484, 500, 512, 520, 530, 533, 540, 548, 558, 580, 581, 584, 591, 598, 604, 634, 638, 659, 660, 662, 670, 682, 690, 740, 760, 776, 780, 784, 796, 798, 858, 862, 882, 887, 949):
            row[''UNSAMP_REGION_GRP_PV''] = 7.0
        elif row[''REGION''] == 300:
            row[''UNSAMP_REGION_GRP_PV''] = 8.0
        elif row[''REGION''] in (292, 620, 621, 911, 912):
            row[''UNSAMP_REGION_GRP_PV''] = 9.0
        elif row[''REGION''] in (470, 792, 901, 902):
            row[''UNSAMP_REGION_GRP_PV''] = 10.0
        elif row[''REGION''] == 372:
            row[''UNSAMP_REGION_GRP_PV''] = 11.0
        elif row[''REGION''] in (831, 832, 833, 931):
            row[''UNSAMP_REGION_GRP_PV''] = 12.0
        elif row[''REGION''] in (921, 923, 924, 926, 933):
            row[''UNSAMP_REGION_GRP_PV''] = 13.0
elif dataset == ''unsampled'':
    if not math.isnan(row[''REGION'']):
        row[''REGION''] = int(row[''REGION''])
        row[''UNSAMP_REGION_GRP_PV''] = row[''REGION'']
if row[''UNSAMP_PORT_GRP_PV''] == ''A201'' and row[''UNSAMP_REGION_GRP_PV''] == 7.0 and row[''ARRIVEDEPART''] == 2:
    row[''UNSAMP_PORT_GRP_PV''] = ''A191''
if row[''UNSAMP_PORT_GRP_PV''] == ''HGS'':
    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''
if row[''UNSAMP_PORT_GRP_PV''] == ''E921'':
    row[''UNSAMP_PORT_GRP_PV''] = ''E911''
if row[''UNSAMP_PORT_GRP_PV''] == ''E951'':
    row[''UNSAMP_PORT_GRP_PV''] = ''E911''
if row[''UNSAMP_PORT_GRP_PV''] == ''A181'' and row[''UNSAMP_REGION_GRP_PV''] == 6.0 and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''A151''
if row[''UNSAMP_PORT_GRP_PV''] == ''A211'' and row[''UNSAMP_REGION_GRP_PV''] == 4.0 and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''A221''
if row[''UNSAMP_PORT_GRP_PV''] == ''A241'' and row[''UNSAMP_REGION_GRP_PV''] == 8.0 and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''A201''
if row[''UNSAMP_PORT_GRP_PV''] == ''RSS'' and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''
if row[''UNSAMP_PORT_GRP_PV''] == ''RSS'' and row[''ARRIVEDEPART''] == 2:
    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 1, 'weekday_end_pv', 'weekday_end_pv', '
if dataset == ''survey'':
    weekday = float(''nan'')
    from datetime import datetime
    day = int(row[''INTDATE''][:2])
    month = int(row[''INTDATE''][2:4])
    year = int(row[''INTDATE''][4:8])
    d = datetime(year,month,day)
    dayweek = (d.isoweekday() + 1) % 7
    if (row[''PORTROUTE''] == 811):
        if (dayweek >= 2 and dayweek <= 5):
            weekday = 1
        else:
            weekday = 2
    else:
        if (dayweek >= 2 and dayweek <= 6):
            weekday = 1
        else:
            weekday = 2
    if (row[''PORTROUTE''] == 811):
        row[''WEEKDAY_END_PV''] = weekday
    elif (row[''PORTROUTE''] >= 600):
        row[''WEEKDAY_END_PV''] = 1
    else:
        row[''WEEKDAY_END_PV''] = weekday
else:
    if (row[''PORTROUTE''] == 811):
        row[''WEEKDAY_END_PV''] = row[''WEEKDAY'']
    elif (row[''PORTROUTE''] >= 600):
        row[''WEEKDAY_END_PV''] = 1
    else:
        row[''WEEKDAY_END_PV''] = row[''WEEKDAY'']
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 2, 'am_pm_night_pv', 'am_pm_night_pv', '
if row[''PORTROUTE''] == 811 and row[''AM_PM_NIGHT''] == 2:
    row[''AM_PM_NIGHT_PV''] = 1
elif row[''PORTROUTE''] == 811 or row[''PORTROUTE''] == 812:
    row[''AM_PM_NIGHT_PV''] = row[''AM_PM_NIGHT'']
else:
    row[''AM_PM_NIGHT_PV''] = 1');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 3, 'mig_flag_pv', 'mig_flag_pv', '
if row[''LOSKEY''] > 0:
    row[''MIG_FLAG_PV''] = 1
else:
    row[''MIG_FLAG_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 17, 'StayImpCtryLevel3_pv', 'StayImpCtryLevel3_pv', '
if row[''STAYIMPCTRYLEVEL2_PV''] in (1,2,4,6,8):
    row[''STAYIMPCTRYLEVEL3_PV''] = 1
elif row[''STAYIMPCTRYLEVEL2_PV''] in (3,5,7):
    row[''STAYIMPCTRYLEVEL3_PV''] = 2
elif row[''STAYIMPCTRYLEVEL2_PV''] in (11,12,13):
    row[''STAYIMPCTRYLEVEL3_PV''] = 3
elif row[''STAYIMPCTRYLEVEL2_PV''] in (14,15):
    row[''STAYIMPCTRYLEVEL3_PV''] = 4
elif row[''STAYIMPCTRYLEVEL2_PV''] in (21,22,23,24,25):
    row[''STAYIMPCTRYLEVEL3_PV''] = 5
elif row[''STAYIMPCTRYLEVEL2_PV''] in (31,32):
    row[''STAYIMPCTRYLEVEL3_PV''] = 6
elif row[''STAYIMPCTRYLEVEL2_PV''] in (41,42):
    row[''STAYIMPCTRYLEVEL3_PV''] = 7
elif row[''STAYIMPCTRYLEVEL2_PV''] in (43,44,45,46):
    row[''STAYIMPCTRYLEVEL3_PV''] = 8
elif row[''STAYIMPCTRYLEVEL2_PV''] in (51,52):
    row[''STAYIMPCTRYLEVEL3_PV''] = 9
elif row[''STAYIMPCTRYLEVEL2_PV''] == 91:
    row[''STAYIMPCTRYLEVEL3_PV''] = 10
else:
    row[''STAYIMPCTRYLEVEL3_PV''] = 99');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 18, 'StayImpCtryLevel4_pv', 'StayImpCtryLevel4_pv', '
if row[''STAYIMPCTRYLEVEL2_PV''] >= 1 and row[''STAYIMPCTRYLEVEL2_PV''] <= 8:
    row[''STAYIMPCTRYLEVEL4_PV''] = 1
elif row[''STAYIMPCTRYLEVEL2_PV''] >= 11 and row[''STAYIMPCTRYLEVEL2_PV''] <= 15:
    row[''STAYIMPCTRYLEVEL4_PV''] = 2
elif row[''STAYIMPCTRYLEVEL2_PV''] >= 21 and row[''STAYIMPCTRYLEVEL2_PV''] <= 32:
    row[''STAYIMPCTRYLEVEL4_PV''] = 3
elif row[''STAYIMPCTRYLEVEL2_PV''] >= 41 and row[''STAYIMPCTRYLEVEL2_PV''] <= 46:
    row[''STAYIMPCTRYLEVEL4_PV''] = 4
else:
    row[''STAYIMPCTRYLEVEL4_PV''] = 5');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 19, 'stay_purpose_grp_pv', 'stay_purpose_grp_pv', '
if row[''PURPOSE''] in (20, 21, 22, 24, 25):
    row[''STAY_PURPOSE_GRP_PV''] = 1
elif row[''PURPOSE''] in (10, 15, 16):
    row[''STAY_PURPOSE_GRP_PV''] = 2
elif row[''PURPOSE''] == 40:
    row[''STAY_PURPOSE_GRP_PV''] = 3
elif row[''PURPOSE''] in (11, 12):
    row[''STAY_PURPOSE_GRP_PV''] = 4
elif row[''PURPOSE''] in (17, 18, 70, 71):
    row[''STAY_PURPOSE_GRP_PV''] = 5
else:
    row[''STAY_PURPOSE_GRP_PV''] = 6');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 20, 'fares_imp_flag_pv', 'fares_imp_flag_pv', '
if (row[''DVFARE''] == 999999) or math.isnan(row[''DVFARE'']) or (row[''DVFARE''] == 0):
    row[''FARES_IMP_FLAG_PV''] = 1
else:
    row[''FARES_IMP_FLAG_PV''] = 0');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 21, 'fares_imp_eligible_pv', 'fares_imp_eligible_pv', '
if (((row[''FAREKEY''] == ''1'') or (row[''FAREKEY''] == ''1.0'')) or ((row[''FARES_IMP_FLAG_PV'']) == 1)) and ((row[''MINS_FLAG_PV'']) == 0):
    row[''FARES_IMP_ELIGIBLE_PV''] = 1
else:
    row[''FARES_IMP_ELIGIBLE_PV''] = 0
');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 22, 'discnt_f1_pv', 'discnt_f1_pv', '
if row[''FLOW''] in (1, 2, 3, 4, 5, 6, 7, 8):
    row[''DISCNT_F1_PV''] = 0.85');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 23, 'discnt_package_cost_pv', 'discnt_package_cost_pv', '
packagecost = None
if row[''PACKAGE''] in (1 ,2):
    if packagecost != 999999:
        if not packagecost==None:
            row[''DISCNT_PACKAGE_COST_PV''] = row[''DISCNT_F1_PV''] * packagecost
    else:
        row[''DISCNT_PACKAGE_COST_PV''] = packagecost
row[''DISCNT_PACKAGE_COST_PV''] = round(row[''DISCNT_PACKAGE_COST_PV''], 1)');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 24, 'discnt_f2_pv', 'discnt_f2_pv', '
if row[''PACKAGE''] in (1,2) and row[''FLOW''] in (1,2,3,4,5,6,7,8):
    row[''DISCNT_F2_PV''] = 0.85');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 25, 'fage_pv', 'fage_pv', '
if row[''KIDAGE''] in (0, 1):
    row[''FAGE_PV''] = 1
elif (row[''KIDAGE''] >= 2) and (row[''KIDAGE''] <= 15):
    row[''FAGE_PV''] = 2
else:
    row[''FAGE_PV''] = 6
if (row[''AGE''] > 1) or math.isnan(row[''AGE'']):
    row[''FAGE_PV''] = 6
elif (row[''AGE''] < 2) and math.isnan(row[''KIDAGE'']):
    row[''FAGE_PV''] = 2');

INSERT INTO ips.PROCESS_VARIABLE_PY (RUN_ID, PROCESS_VARIABLE_ID, PV_NAME, PV_DESC, PV_DEF)
VALUES ('TEMPLATE', 26, 'type_pv', 'type_pv', '
if row[''PURPOSE''] in (20,21,22):
    row[''TYPE_PV''] = 1
else:
    row[''TYPE_PV''] = 2');

-- create table PROCESS_VARIABLE_PY_BACKUP
-- (
--     RUN_ID              varchar(40)   not null,
--     PROCESS_VARIABLE_ID decimal       not null,
--     PV_NAME             varchar(30)   not null,
--     PV_DESC             varchar(1000) not null,
--     PV_DEF              text          not null
-- );
--
--
-- create table PROCESS_VARIABLE_PY_BACKUP_2
-- (
--     RUN_ID              varchar(40)   not null,
--     PROCESS_VARIABLE_ID decimal       not null,
--     PV_NAME             varchar(30)   not null,
--     PV_DESC             varchar(1000) not null,
--     PV_DEF              text          not null
-- );


create table PROCESS_VARIABLE_SET
(
    RUN_ID varchar(40) not null,
    NAME   varchar(30) not null,
    USER   varchar(50) null,
    PERIOD varchar(12) not null,
    YEAR   year(4)     not null
);
INSERT INTO PROCESS_VARIABLE_SET(run_id, name, user, period, year)
VALUES ('TEMPLATE', 'Template', 'Template', '12', 2017);


create table PROCESS_VARIABLE_TESTING
(
    RUN_ID              varchar(40)   not null,
    PROCESS_VARIABLE_ID decimal       not null,
    PV_NAME             varchar(30)   not null,
    PV_DESC             varchar(1000) not null,
    PV_DEF              text          not null
);


create table PS_FINAL
(
    RUN_ID            varchar(40)    not null,
    SERIAL            decimal(15)    not null,
    SHIFT_WT          decimal(9, 3)  null,
    NON_RESPONSE_WT   decimal(9, 3)  null,
    MINS_WT           decimal(9, 3)  null,
    TRAFFIC_WT        decimal(9, 3)  null,
    UNSAMP_TRAFFIC_WT decimal(9, 3)  null,
    IMBAL_WT          decimal(9, 3)  null,
    FINAL_WT          decimal(12, 3) null
);


create table PS_IMBALANCE
(
    RUN_ID       varchar(40)    not null,
    FLOW         decimal(2)     null,
    SUM_PRIOR_WT decimal(12, 3) null,
    SUM_IMBAL_WT decimal(12, 3) null
);
create table PS_INSTRUCTION
(
    PN_ID          decimal       not null,
    PS_INSTRUCTION varchar(2000) not null
);


create table PS_MINIMUMS
(
    RUN_ID                varchar(40)    not null,
    MINS_PORT_GRP_PV      decimal(3)     null,
    ARRIVEDEPART          decimal(1)     null,
    MINS_CTRY_GRP_PV      decimal(6)     null,
    MINS_NAT_GRP_PV       decimal(6)     null,
    MINS_CTRY_PORT_GRP_PV varchar(10)    null,
    MINS_CASES            decimal(6)     null,
    FULLS_CASES           decimal(6)     null,
    PRIOR_GROSS_MINS      decimal(12, 3) null,
    PRIOR_GROSS_FULLS     decimal(12, 3) null,
    PRIOR_GROSS_ALL       decimal(12, 3) null,
    MINS_WT               decimal(9, 3)  null,
    POST_SUM              decimal(12, 3) null,
    CASES_CARRIED_FWD     decimal(6)     null
);


create table PS_NON_RESPONSE
(
    RUN_ID           varchar(40)    not null,
    NR_PORT_GRP_PV   decimal(3)     not null,
    ARRIVEDEPART     decimal(1)     not null,
    WEEKDAY_END_PV   decimal(1)     null,
    MEAN_RESPS_SH_WT decimal(9, 3)  null,
    COUNT_RESPS      decimal(6)     null,
    PRIOR_SUM        decimal(12, 3) null,
    GROSS_RESP       decimal(12, 3) null,
    GNR              decimal(12, 3) null,
    MEAN_NR_WT       decimal(9, 3)  null
);


create table PS_SHIFT_DATA
(
    RUN_ID            varchar(40)    not null,
    SHIFT_PORT_GRP_PV varchar(10)    not null,
    ARRIVEDEPART      decimal(1)     not null,
    WEEKDAY_END_PV    decimal(1)     null,
    AM_PM_NIGHT_PV    decimal(1)     null,
    MIGSI             int            null,
    POSS_SHIFT_CROSS  decimal(5)     null,
    SAMP_SHIFT_CROSS  decimal(5)     null,
    MIN_SH_WT         decimal(9, 3)  null,
    MEAN_SH_WT        decimal(9, 3)  null,
    MAX_SH_WT         decimal(9, 3)  null,
    COUNT_RESPS       decimal(6)     null,
    SUM_SH_WT         decimal(12, 3) null
);


create table PS_TRAFFIC
(
    RUN_ID             varchar(40)    not null,
    SAMP_PORT_GRP_PV   varchar(10)    not null,
    ARRIVEDEPART       decimal(1)     not null,
    FOOT_OR_VEHICLE_PV decimal(2)     null,
    CASES              decimal(6)     null,
    TRAFFICTOTAL       decimal(12, 3) null,
    SUM_TRAFFIC_WT     decimal(12, 3) null,
    TRAFFIC_WT         decimal(9, 3)  null
);


create table PS_UNSAMPLED_OOH
(
    RUN_ID                varchar(40)    not null,
    UNSAMP_PORT_GRP_PV    varchar(10)    not null,
    ARRIVEDEPART          decimal(1)     not null,
    UNSAMP_REGION_GRP_PV  decimal(9, 3)  null,
    CASES                 decimal(6)     null,
    SUM_PRIOR_WT          decimal(12, 3) null,
    SUM_UNSAMP_TRAFFIC_WT decimal(12, 3) null,
    UNSAMP_TRAFFIC_WT     decimal(9, 3)  null
);


-- create table QUERY_RESPONSE
-- (
--     TASK_ID       varchar(40) not null,
--     RESPONSE_CODE varchar(10) null,
--     RESPONSE_MSG  text        null
-- );


create table RESPONSE
(
    RUN_ID        varchar(40)                        not null,
    STEP_NUMBER   int                                not null,
    RESPONSE_CODE int                                not null,
    MESSAGE       varchar(250)                       null,
    TIME_STAMP    datetime default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP
);


-- create table RESPONSE_ARCHIVE
-- (
--     RUN_ID        varchar(40)   not null,
--     STEP_NUMBER   int           not null,
--     RESPONSE_CODE int           not null,
--     MESSAGE       varchar(250)  null,
--     OUTPUT        varchar(4000) null,
--     TIME_STAMP    datetime      null
-- );

create table RUN
(
    RUN_ID        varchar(40)                          not null,
    RUN_NAME      varchar(30)                          null,
    RUN_DESC      varchar(250)                         null,
    USER_ID       varchar(20)                          null,
    YEAR          year                                 null,
    PERIOD        varchar(255)                         null,
    RUN_STATUS    decimal(2) default 0                 null,
    RUN_TYPE_ID   decimal(3) default 0                 null,
    LAST_MODIFIED timestamp  default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    STEP          varchar(255) charset utf8            null,
    PERCENT       int        default 0                 null,
    constraint RUN_RUN_ID_uindex
        unique (RUN_ID)
);

alter table RUN
    add primary key (RUN_ID);


-- create table RUN_DATA_MAP
-- (
--     RUN_ID      varchar(40) not null,
--     VERSION_ID  decimal     not null,
--     DATA_SOURCE varchar(60) not null,
--     constraint RUN_DATA_MAP_RUN_ID_UINDEX
--         unique (RUN_ID)
-- );

-- alter table RUN_DATA_MAP
--     add primary key (RUN_ID);


create table RUN_STEPS
(
    RUN_ID      varchar(40) not null,
    STEP_NUMBER decimal(2)  not null,
    STEP_NAME   varchar(80) not null,
    STEP_STATUS decimal(2)  not null
);


-- create table RUN_TYPE
-- (
--     RUN_TYPE_ID         decimal(3)  not null,
--     RUN_TYPE_NAME       varchar(30) not null,
--     RUN_TYPE_DEFINITION text        not null
-- );


create table SAS_AIR_MILES
(
    SERIAL    decimal(15) not null,
    DIRECTLEG decimal(6)  null,
    OVLEG     decimal(6)  null,
    UKLEG     decimal(6)  null
);


-- create table SAS_DATA_EXPORT
-- (
--     SAS_PROCESS_ID decimal     not null,
--     SDE_LABEL      varchar(80) not null,
--     SDE_DATA       binary(1)   not null
-- );


create table SAS_FARES_IMP
(
    SERIAL         decimal(15) not null,
    FARE           decimal(6)  null,
    FAREK          decimal(2)  null,
    SPEND          decimal(7)  null,
    OPERA_PV       decimal(1)  null,
    SPENDIMPREASON decimal(1)  null
);


create table SAS_FARES_SPV
(
    SERIAL                 decimal(15)   not null,
    FARES_IMP_FLAG_PV      decimal(1)    null,
    FARES_IMP_ELIGIBLE_PV  decimal(1)    null,
    DISCNT_PACKAGE_COST_PV decimal(6)    null,
    DISCNT_F1_PV           decimal(4, 3) null,
    DISCNT_F2_PV           decimal(4, 3) null,
    FAGE_PV                decimal(2)    null,
    TYPE_PV                float(9, 3)   null,
    UKPORT1_PV             float(9, 3)   null,
    UKPORT2_PV             float(9, 3)   null,
    UKPORT3_PV             float(9, 3)   null,
    UKPORT4_PV             float(9, 3)   null,
    OSPORT1_PV             decimal(8)    null,
    OSPORT2_PV             decimal(8)    null,
    OSPORT3_PV             float(9, 3)   null,
    OSPORT4_PV             float(9, 3)   null,
    APD_PV                 decimal(4)    null,
    QMFARE_PV              decimal(8)    null,
    DUTY_FREE_PV           decimal(4)    null
);


create table SAS_FINAL_WT
(
    SERIAL   decimal(15)    not null,
    FINAL_WT decimal(12, 3) null
);


create table SAS_IMBALANCE_SPV
(
    SERIAL               decimal(15)   not null,
    IMBAL_PORT_GRP_PV    decimal(3)    null,
    IMBAL_PORT_SUBGRP_PV decimal(3)    null,
    IMBAL_PORT_FACT_PV   decimal(5, 3) null,
    IMBAL_CTRY_GRP_PV    decimal(3)    null,
    IMBAL_CTRY_FACT_PV   decimal(5, 3) null,
    IMBAL_ELIGIBLE_PV    decimal(1)    null,
    PURPOSE_PV           decimal(8)    null,
    FLOW_PV              decimal(2)    null
);


create table SAS_IMBALANCE_WT
(
    SERIAL   decimal(15)   not null,
    IMBAL_WT decimal(9, 3) null
);


create table SAS_MINIMUMS_SPV
(
    SERIAL                decimal(15) not null,
    MINS_PORT_GRP_PV      decimal(3)  null,
    MINS_CTRY_GRP_PV      decimal(6)  null,
    MINS_NAT_GRP_PV       decimal(6)  null,
    MINS_CTRY_PORT_GRP_PV varchar(10) null,
    MINS_QUALITY_PV       decimal(1)  null,
    MINS_FLAG_PV          decimal(1)  null
);


create table SAS_MINIMUMS_WT
(
    SERIAL  decimal(15)   not null,
    MINS_WT decimal(9, 3) null
);


create table SAS_NON_RESPONSE_DATA
(
    REC_ID         int auto_increment
        primary key,
    PORTROUTE      decimal(4) not null,
    WEEKDAY        decimal(1) null,
    ARRIVEDEPART   decimal(1) null,
    AM_PM_NIGHT    decimal(1) null,
    SAMPINTERVAL   decimal(4) null,
    MIGTOTAL       decimal    null,
    ORDTOTAL       decimal    null,
    NR_PORT_GRP_PV decimal(3) null,
    WEEKDAY_END_PV decimal(1) null,
    AM_PM_NIGHT_PV decimal(1) null
);


create table SAS_NON_RESPONSE_PV
(
    REC_ID         decimal    not null,
    WEEKDAY_END_PV decimal(1) null,
    NR_PORT_GRP_PV decimal(3) null
);


create table SAS_NON_RESPONSE_SPV
(
    SERIAL         decimal(15) not null,
    NR_PORT_GRP_PV decimal(3)  null,
    MIG_FLAG_PV    decimal(1)  null,
    NR_FLAG_PV     decimal(1)  null
);


create table SAS_NON_RESPONSE_WT
(
    SERIAL          decimal(15)   not null,
    NON_RESPONSE_WT decimal(9, 3) null
);


-- create table SAS_PARAMETERS
-- (
--     PARAMETER_SET_ID decimal       not null,
--     PARAMETER_NAME   varchar(32)   not null,
--     PARAMETER_VALUE  varchar(4000) null
-- );


create table SAS_PROCESS_VARIABLE
(
    PROCVAR_NAME  varchar(30) not null,
    PROCVAR_RULE  text        not null,
    PROCVAR_ORDER decimal(2)  not null
);


create table SAS_PS_FINAL
(
    SERIAL            decimal(15)    not null,
    SHIFT_WT          decimal(9, 3)  null,
    NON_RESPONSE_WT   decimal(9, 3)  null,
    MINS_WT           decimal(9, 3)  null,
    TRAFFIC_WT        decimal(9, 3)  null,
    UNSAMP_TRAFFIC_WT decimal(9, 3)  null,
    IMBAL_WT          decimal(9, 3)  null,
    FINAL_WT          decimal(12, 3) null
);


create table SAS_PS_IMBALANCE
(
    FLOW         decimal(2)     null,
    SUM_PRIOR_WT decimal(12, 3) null,
    SUM_IMBAL_WT decimal(12, 3) null
);


create table SAS_PS_MINIMUMS
(
    MINS_PORT_GRP_PV      decimal(3)     null,
    ARRIVEDEPART          decimal(1)     null,
    MINS_CTRY_GRP_PV      decimal(6)     null,
    MINS_NAT_GRP_PV       decimal(6)     null,
    MINS_CTRY_PORT_GRP_PV varchar(10)    null,
    MINS_CASES            decimal(6)     null,
    FULLS_CASES           decimal(6)     null,
    PRIOR_GROSS_MINS      decimal(12, 3) null,
    PRIOR_GROSS_FULLS     decimal(12, 3) null,
    PRIOR_GROSS_ALL       decimal(12, 3) null,
    MINS_WT               decimal(9, 3)  null,
    POST_SUM              decimal(12, 3) null,
    CASES_CARRIED_FWD     decimal(6)     null
);


create table SAS_PS_NON_RESPONSE
(
    NR_PORT_GRP_PV   decimal(3)     not null,
    ARRIVEDEPART     decimal(1)     not null,
    WEEKDAY_END_PV   decimal(1)     null,
    MEAN_RESPS_SH_WT decimal(9, 3)  null,
    COUNT_RESPS      decimal(6)     null,
    PRIOR_SUM        decimal(12, 3) null,
    GROSS_RESP       decimal(12, 3) null,
    GNR              decimal(12, 3) null,
    MEAN_NR_WT       decimal(9, 3)  null
);


create table SAS_PS_SHIFT_DATA
(
    SHIFT_PORT_GRP_PV varchar(10)    not null,
    ARRIVEDEPART      decimal(1)     not null,
    WEEKDAY_END_PV    decimal(1)     null,
    AM_PM_NIGHT_PV    decimal(1)     null,
    MIGSI             int            null,
    POSS_SHIFT_CROSS  decimal(5)     null,
    SAMP_SHIFT_CROSS  decimal(5)     null,
    MIN_SH_WT         decimal(9, 3)  null,
    MEAN_SH_WT        decimal(9, 3)  null,
    MAX_SH_WT         decimal(9, 3)  null,
    COUNT_RESPS       decimal(6)     null,
    SUM_SH_WT         decimal(12, 3) null
);


create table SAS_PS_TRAFFIC
(
    SAMP_PORT_GRP_PV   varchar(10)    not null,
    ARRIVEDEPART       decimal(1)     not null,
    FOOT_OR_VEHICLE_PV decimal(2)     null,
    CASES              decimal(6)     null,
    TRAFFICTOTAL       decimal(12, 3) null,
    SUM_TRAFFIC_WT     decimal(12, 3) null,
    TRAFFIC_WT         decimal(9, 3)  null
);


create table SAS_PS_UNSAMPLED_OOH
(
    UNSAMP_PORT_GRP_PV    varchar(10)    not null,
    ARRIVEDEPART          decimal(1)     not null,
    UNSAMP_REGION_GRP_PV  decimal(9, 3)  null,
    CASES                 decimal(6)     null,
    SUM_PRIOR_WT          decimal(12, 3) null,
    SUM_UNSAMP_TRAFFIC_WT decimal(12, 3) null,
    UNSAMP_TRAFFIC_WT     decimal(9, 3)  null
);


create table SAS_RAIL_IMP
(
    SERIAL decimal(15) not null,
    SPEND  decimal(7)  null
);


create table SAS_RAIL_SPV
(
    SERIAL               decimal(15) not null,
    RAIL_CNTRY_GRP_PV    decimal(3)  null,
    RAIL_EXERCISE_PV     decimal(6)  null,
    RAIL_IMP_ELIGIBLE_PV decimal(1)  null
);


create table SAS_REGIONAL_IMP
(
    SERIAL          decimal(15)   not null,
    VISIT_WT        decimal(6, 3) null,
    STAY_WT         decimal(6, 3) null,
    EXPENDITURE_WT  decimal(6, 3) null,
    VISIT_WTK       float   null,
    STAY_WTK        float   null,
    EXPENDITURE_WTK float  null,
    NIGHTS1         decimal(3)    null,
    NIGHTS2         decimal(3)    null,
    NIGHTS3         decimal(3)    null,
    NIGHTS4         decimal(3)    null,
    NIGHTS5         decimal(3)    null,
    NIGHTS6         decimal(3)    null,
    NIGHTS7         decimal(3)    null,
    NIGHTS8         decimal(3)    null,
    STAY1K          varchar(10)   null,
    STAY2K          varchar(10)   null,
    STAY3K          varchar(10)   null,
    STAY4K          varchar(10)   null,
    STAY5K          varchar(10)   null,
    STAY6K          varchar(10)   null,
    STAY7K          varchar(10)   null,
    STAY8K          varchar(10)   null
);


create table SAS_REGIONAL_SPV
(
    SERIAL               decimal(15) not null,
    PURPOSE_PV           decimal(8)  null,
    STAYIMPCTRYLEVEL1_PV decimal(8)  null,
    STAYIMPCTRYLEVEL2_PV decimal(8)  null,
    STAYIMPCTRYLEVEL3_PV decimal(8)  null,
    STAYIMPCTRYLEVEL4_PV decimal(8)  null,
    REG_IMP_ELIGIBLE_PV  decimal(1)  null
);


-- create table SAS_RESPONSE
-- (
--     SAS_PROCESS_ID decimal       not null,
--     RESPONSE_CODE  decimal(5)    not null,
--     ERROR_MSG      varchar(250)  null,
--     STACK_TRACE    varchar(4000) null,
--     WARNINGS       varchar(4000) null,
--     TIME_STAMP     datetime      null
-- );


create table SAS_SHIFT_DATA
(
    REC_ID            int auto_increment
        primary key,
    PORTROUTE         decimal(4)  not null,
    WEEKDAY           decimal(1)  not null,
    ARRIVEDEPART      decimal(1)  not null,
    TOTAL             decimal     not null,
    AM_PM_NIGHT       decimal(1)  not null,
    SHIFT_PORT_GRP_PV varchar(10) null,
    AM_PM_NIGHT_PV    decimal(1)  null,
    WEEKDAY_END_PV    decimal(1)  null
);


-- create table SAS_SHIFT_DATA_RICER
-- (
--     REC_ID            decimal     not null,
--     PORTROUTE         decimal(4)  not null,
--     WEEKDAY           decimal(1)  not null,
--     ARRIVEDEPART      decimal(1)  not null,
--     TOTAL             decimal     not null,
--     AM_PM_NIGHT       decimal(1)  not null,
--     SHIFT_PORT_GRP_PV varchar(10) null,
--     AM_PM_NIGHT_PV    decimal(1)  null,
--     WEEKDAY_END_PV    decimal(1)  null
-- );


create table SAS_SHIFT_PV
(
    REC_ID            decimal     not null,
    SHIFT_PORT_GRP_PV varchar(10) null,
    AM_PM_NIGHT_PV    decimal(1)  null,
    WEEKDAY_END_PV    decimal(1)  null
);


create table SAS_SHIFT_SPV
(
    SERIAL            decimal(15) not null,
    SHIFT_PORT_GRP_PV varchar(10) null,
    AM_PM_NIGHT_PV    decimal(1)  null,
    WEEKDAY_END_PV    decimal(1)  null,
    SHIFT_FLAG_PV     decimal(1)  null,
    CROSSINGS_FLAG_PV decimal(1)  null,
    constraint SAS_SHIFT_SPV_pk
        unique (SERIAL)
);


create table SAS_SHIFT_WT
(
    SERIAL   decimal(15)   not null,
    SHIFT_WT decimal(9, 3) null
);


create table SAS_SPEND_IMP
(
    SERIAL   decimal(15) not null
        primary key,
    NEWSPEND decimal(7)  null,
    SPENDK   decimal(2)  null
);


create table SAS_SPEND_SPV
(
    SERIAL                decimal(15) not null,
    SPEND_IMP_FLAG_PV     decimal(1)  null,
    SPEND_IMP_ELIGIBLE_PV decimal(1)  null,
    UK_OS_PV              decimal(2)  null,
    PUR1_PV               decimal(8)  null,
    PUR2_PV               decimal(8)  null,
    PUR3_PV               decimal(8)  null,
    DUR1_PV               decimal(8)  null,
    DUR2_PV               decimal(8)  null
);


create table SAS_STAY_IMP
(
    SERIAL decimal(15) not null
        primary key,
    STAY   decimal(3)  null,
    STAYK  decimal(1)  null
);


create table SAS_STAY_SPV
(
    SERIAL               decimal(15) not null,
    STAY_IMP_FLAG_PV     decimal(1)  null,
    STAY_IMP_ELIGIBLE_PV decimal(1)  null,
    STAYIMPCTRYLEVEL1_PV decimal(8)  null,
    STAYIMPCTRYLEVEL2_PV decimal(8)  null,
    STAYIMPCTRYLEVEL3_PV decimal(8)  null,
    STAYIMPCTRYLEVEL4_PV decimal(8)  null,
    STAY_PURPOSE_GRP_PV  decimal(2)  null
);

create index SAS_STAY_SPV_SERIAL_index
    on SAS_STAY_SPV (SERIAL);


-- create table SAS_SURVEY_COLUMN
-- (
--     VERSION_ID    decimal     not null,
--     COLUMN_NO     decimal(4)  not null,
--     COLUMN_DESC   varchar(30) not null,
--     COLUMN_TYPE   varchar(20) not null,
--     COLUMN_LENGTH decimal(5)  not null
-- );

create table SAS_SURVEY_SUBSAMPLE
(
    SERIAL                 decimal(15)    not null,
    AGE                    decimal(3)     null,
    AM_PM_NIGHT            decimal(1)     null,
    ANYUNDER16             varchar(2)     null,
    APORTLATDEG            decimal(2)     null,
    APORTLATMIN            decimal(2)     null,
    APORTLATSEC            decimal(2)     null,
    APORTLATNS             varchar(1)     null,
    APORTLONDEG            decimal(3)     null,
    APORTLONMIN            decimal(2)     null,
    APORTLONSEC            decimal(2)     null,
    APORTLONEW             varchar(1)     null,
    ARRIVEDEPART           decimal(1)     null,
    BABYFARE               decimal(4, 2)  null,
    BEFAF                  decimal(6)     null,
    CHANGECODE             decimal(6)     null,
    CHILDFARE              decimal(4, 2)  null,
    COUNTRYVISIT           decimal(4)     null,
    CPORTLATDEG            decimal(2)     null,
    CPORTLATMIN            decimal(2)     null,
    CPORTLATSEC            decimal(2)     null,
    CPORTLATNS             varchar(1)     null,
    CPORTLONDEG            decimal(3)     null,
    CPORTLONMIN            decimal(2)     null,
    CPORTLONSEC            decimal(2)     null,
    CPORTLONEW             varchar(3)     null,
    INTDATE                varchar(8)     null,
    DAYTYPE                decimal(1)     null,
    DIRECTLEG              decimal(6)     null,
    DVEXPEND               decimal(6)     null,
    DVFARE                 decimal(6)     null,
    DVLINECODE             decimal(6)     null,
    DVPACKAGE              decimal(1)     null,
    PACKAGECOST            float          null,
    DVPACKCOST             decimal(6)     null,
    DVPERSONS              decimal(3)     null,
    DVPORTCODE             decimal(6)     null,
    EXPENDCODE             varchar(4)     null,
    EXPENDITURE            decimal(6)     null,
    FARE                   decimal(6)     null,
    FAREK                  decimal(2)     null,
    FLOW                   decimal(2)     null,
    HAULKEY                decimal(2)     null,
    INTENDLOS              decimal(2)     null,
    KIDAGE                 decimal(2)     null,
    LOSKEY                 decimal(2)     null,
    MAINCONTRA             decimal(1)     null,
    MIGSI                  int            null,
    INTMONTH               decimal(2)     null,
    NATIONALITY            decimal(4)     null,
    NATIONNAME             varchar(50)    null,
    NIGHTS1                decimal(3)     null,
    NIGHTS2                decimal(3)     null,
    NIGHTS3                decimal(3)     null,
    NIGHTS4                decimal(3)     null,
    NIGHTS5                decimal(3)     null,
    NIGHTS6                decimal(3)     null,
    NIGHTS7                decimal(3)     null,
    NIGHTS8                decimal(3)     null,
    NUMADULTS              decimal(3)     null,
    NUMDAYS                decimal(3)     null,
    NUMNIGHTS              decimal(3)     null,
    NUMPEOPLE              decimal(3)     null,
    PACKAGEHOL             decimal(1)     null,
    PACKAGEHOLUK           decimal(1)     null,
    PERSONS                decimal(2)     null,
    PORTROUTE              decimal(4)     null,
    PACKAGE                decimal(2)     null,
    PROUTELATDEG           decimal(2)     null,
    PROUTELATMIN           decimal(2)     null,
    PROUTELATSEC           decimal(2)     null,
    PROUTELATNS            varchar(1)     null,
    PROUTELONDEG           decimal(3)     null,
    PROUTELONMIN           decimal(2)     null,
    PROUTELONSEC           decimal(2)     null,
    PROUTELONEW            varchar(1)     null,
    PURPOSE                decimal(2)     null,
    QUARTER                decimal(1)     null,
    RESIDENCE              decimal(4)     null,
    RESPNSE                decimal(2)     null,
    SEX                    decimal(1)     null,
    SHIFTNO                decimal(6)     null,
    SHUTTLE                decimal(1)     null,
    SINGLERETURN           decimal(1)     null,
    TANDTSI                decimal(8)     null,
    TICKETCOST             decimal(6)     null,
    TOWNCODE1              decimal(6)     null,
    TOWNCODE2              decimal(6)     null,
    TOWNCODE3              decimal(6)     null,
    TOWNCODE4              decimal(6)     null,
    TOWNCODE5              decimal(6)     null,
    TOWNCODE6              decimal(6)     null,
    TOWNCODE7              decimal(6)     null,
    TOWNCODE8              decimal(6)     null,
    TRANSFER               decimal(6)     null,
    UKFOREIGN              decimal(1)     null,
    VEHICLE                decimal(1)     null,
    VISITBEGAN             varchar(8)     null,
    WELSHNIGHTS            decimal(3)     null,
    WELSHTOWN              decimal(6)     null,
    AM_PM_NIGHT_PV         decimal(1)     null,
    APD_PV                 decimal(4)     null,
    ARRIVEDEPART_PV        decimal(1)     null,
    CROSSINGS_FLAG_PV      decimal(1)     null,
    STAYIMPCTRYLEVEL1_PV   decimal(8)     null,
    STAYIMPCTRYLEVEL2_PV   decimal(8)     null,
    STAYIMPCTRYLEVEL3_PV   decimal(8)     null,
    STAYIMPCTRYLEVEL4_PV   decimal(8)     null,
    DAY_PV                 decimal(2)     null,
    DISCNT_F1_PV           decimal(4, 3)  null,
    DISCNT_F2_PV           decimal(4, 3)  null,
    DISCNT_PACKAGE_COST_PV decimal(6)     null,
    DUR1_PV                decimal(3)     null,
    DUR2_PV                decimal(3)     null,
    DUTY_FREE_PV           decimal(4)     null,
    FAGE_PV                decimal(2)     null,
    FARES_IMP_ELIGIBLE_PV  decimal(1)     null,
    FARES_IMP_FLAG_PV      decimal(1)     null,
    FLOW_PV                decimal(2)     null,
    FOOT_OR_VEHICLE_PV     decimal(2)     null,
    HAUL_PV                varchar(2)     null,
    IMBAL_CTRY_FACT_PV     decimal(5, 3)  null,
    IMBAL_CTRY_GRP_PV      decimal(3)     null,
    IMBAL_ELIGIBLE_PV      decimal(1)     null,
    IMBAL_PORT_FACT_PV     decimal(5, 3)  null,
    IMBAL_PORT_GRP_PV      decimal(3)     null,
    IMBAL_PORT_SUBGRP_PV   decimal(3)     null,
    LOS_PV                 decimal(3)     null,
    LOSDAYS_PV             decimal(3)     null,
    MIG_FLAG_PV            decimal(1)     null,
    MINS_CTRY_GRP_PV       decimal(6)     null,
    MINS_CTRY_PORT_GRP_PV  varchar(10)    null,
    MINS_FLAG_PV           decimal(1)     null,
    MINS_NAT_GRP_PV        decimal(6)     null,
    MINS_PORT_GRP_PV       decimal(3)     null,
    MINS_QUALITY_PV        decimal(1)     null,
    NR_FLAG_PV             decimal(1)     null,
    NR_PORT_GRP_PV         decimal(3)     null,
    OPERA_PV               decimal(2)     null,
    OSPORT1_PV             decimal(8)     null,
    OSPORT2_PV             decimal(8)     null,
    OSPORT3_PV             decimal(8)     null,
    OSPORT4_PV             decimal(8)     null,
    PUR1_PV                decimal(8)     null,
    PUR2_PV                decimal(8)     null,
    PUR3_PV                decimal(8)     null,
    PURPOSE_PV             decimal(8)     null,
    QMFARE_PV              decimal(8)     null,
    RAIL_CNTRY_GRP_PV      decimal(3)     null,
    RAIL_EXERCISE_PV       decimal(6)     null,
    RAIL_IMP_ELIGIBLE_PV   decimal(1)     null,
    REG_IMP_ELIGIBLE_PV    decimal(1)     null,
    SAMP_PORT_GRP_PV       varchar(10)    null,
    SHIFT_FLAG_PV          decimal(1)     null,
    SHIFT_PORT_GRP_PV      varchar(10)    null,
    SPEND_IMP_FLAG_PV      decimal(1)     null,
    SPEND_IMP_ELIGIBLE_PV  decimal(1)     null,
    STAY_IMP_ELIGIBLE_PV   decimal(1)     null,
    STAY_IMP_FLAG_PV       decimal(1)     null,
    STAY_PURPOSE_GRP_PV    decimal(2)     null,
    TOWNCODE_PV            varchar(10)    null,
    TOWN_IMP_ELIGIBLE_PV   decimal(1)     null,
    TYPE_PV                float(9, 3)    null,
    UK_OS_PV               decimal(1)     null,
    UKPORT1_PV             decimal(8)     null,
    UKPORT2_PV             decimal(8)     null,
    UKPORT3_PV             decimal(8)     null,
    UKPORT4_PV             decimal(8)     null,
    UNSAMP_PORT_GRP_PV     varchar(10)    null,
    UNSAMP_REGION_GRP_PV   decimal(9, 3)  null,
    WEEKDAY_END_PV         decimal(1)     null,
    DIRECT                 decimal(6)     null,
    EXPENDITURE_WT         decimal(6, 3)  null,
    EXPENDITURE_WTK        float    null,
    FAREKEY                varchar(4)     null,
    OVLEG                  decimal(6)     null,
    SPEND                  decimal(7)     null,
    SPEND1                 decimal(7)     null,
    SPEND2                 decimal(7)     null,
    SPEND3                 decimal(7)     null,
    SPEND4                 decimal(7)     null,
    SPEND5                 decimal(7)     null,
    SPEND6                 decimal(7)     null,
    SPEND7                 decimal(7)     null,
    SPEND8                 decimal(7)     null,
    SPEND9                 decimal(7)     null,
    SPENDIMPREASON         decimal(1)     null,
    SPENDK                 decimal(2)     null,
    STAY                   decimal(3)     null,
    STAYK                  decimal(1)     null,
    STAY1K                 varchar(10)    null,
    STAY2K                 varchar(10)    null,
    STAY3K                 varchar(10)    null,
    STAY4K                 varchar(10)    null,
    STAY5K                 varchar(10)    null,
    STAY6K                 varchar(10)    null,
    STAY7K                 varchar(10)    null,
    STAY8K                 varchar(10)    null,
    STAY9K                 varchar(10)    null,
    STAYTLY                decimal(6)     null,
    STAY_WT                decimal(6, 3)  null,
    STAY_WTK               float    null,
    TYPEINTERVIEW          decimal(3)     null,
    UKLEG                  decimal(6)     null,
    VISIT_WT               decimal(6, 3)  null,
    VISIT_WTK              float    null,
    SHIFT_WT               decimal(9, 3)  null,
    NON_RESPONSE_WT        decimal(9, 3)  null,
    MINS_WT                decimal(9, 3)  null,
    TRAFFIC_WT             decimal(9, 3)  null,
    UNSAMP_TRAFFIC_WT      decimal(9, 3)  null,
    IMBAL_WT               decimal(9, 3)  null,
    FINAL_WT               decimal(12, 3) null,
    constraint SAS_SURVEY_SUBSAMPLE_pk
        unique (SERIAL)
);

-- create table SAS_SURVEY_VALUE
-- (
--     VERSION_ID   decimal      not null,
--     SERIAL_NO    decimal(15)  not null,
--     COLUMN_NO    decimal(4)   not null,
--     COLUMN_VALUE varchar(100) not null
-- );


create table SAS_TOWN_STAY_IMP
(
    SERIAL decimal(15) not null,
    SPEND1 decimal(7)  null,
    SPEND2 decimal(7)  null,
    SPEND3 decimal(7)  null,
    SPEND4 decimal(7)  null,
    SPEND5 decimal(7)  null,
    SPEND6 decimal(7)  null,
    SPEND7 decimal(7)  null,
    SPEND8 decimal(7)  null
);


create table SAS_TOWN_STAY_SPV
(
    SERIAL               decimal(15) not null,
    PURPOSE_PV           decimal(8)  null,
    STAYIMPCTRYLEVEL1_PV decimal(8)  null,
    STAYIMPCTRYLEVEL2_PV decimal(8)  null,
    STAYIMPCTRYLEVEL3_PV decimal(8)  null,
    STAYIMPCTRYLEVEL4_PV decimal(8)  null,
    TOWN_IMP_ELIGIBLE_PV decimal(1)  null
);


create table SAS_TRAFFIC_DATA
(
    REC_ID             int auto_increment
        primary key,
    PORTROUTE          decimal(4)  null,
    ARRIVEDEPART       decimal(1)  null,
    TRAFFICTOTAL       decimal     null,
    PERIODSTART        varchar(10) null,
    PERIODEND          varchar(10) null,
    AM_PM_NIGHT        decimal(1)  null,
    HAUL               varchar(2)  null,
    VEHICLE            decimal(1)  null,
    SAMP_PORT_GRP_PV   varchar(10) null,
    FOOT_OR_VEHICLE_PV decimal(2)  null,
    HAUL_PV            varchar(2)  null
);


create table SAS_TRAFFIC_PV
(
    REC_ID             decimal     not null,
    SAMP_PORT_GRP_PV   varchar(10) null,
    FOOT_OR_VEHICLE_PV decimal(2)  null,
    HAUL_PV            varchar(2)  null
);


create table SAS_TRAFFIC_SPV
(
    SERIAL             decimal(15) not null,
    SAMP_PORT_GRP_PV   varchar(10) null,
    FOOT_OR_VEHICLE_PV decimal(2)  null,
    HAUL_PV            varchar(2)  null
);


create table SAS_TRAFFIC_WT
(
    SERIAL     decimal(15)   not null,
    TRAFFIC_WT decimal(9, 3) null
);


create table SAS_UNSAMPLED_OOH_DATA
(
    REC_ID               int auto_increment
        primary key,
    PORTROUTE            decimal(4)    null,
    REGION               decimal(3)    null,
    ARRIVEDEPART         decimal(1)    null,
    UNSAMP_TOTAL         decimal       null,
    UNSAMP_PORT_GRP_PV   varchar(10)   null,
    UNSAMP_REGION_GRP_PV decimal(9, 3) null
);


create table SAS_UNSAMPLED_OOH_PV
(
    REC_ID               decimal       not null,
    UNSAMP_PORT_GRP_PV   varchar(10)   not null,
    UNSAMP_REGION_GRP_PV decimal(9, 3) null,
    HAUL_PV              varchar(2)    null
);


create table SAS_UNSAMPLED_OOH_SPV
(
    SERIAL               decimal(15)   not null,
    UNSAMP_PORT_GRP_PV   varchar(10)   null,
    UNSAMP_REGION_GRP_PV decimal(9, 3) null,
    HAUL_PV              varchar(2)    null
);


create table SAS_UNSAMPLED_OOH_WT
(
    SERIAL            decimal(15)   not null,
    UNSAMP_TRAFFIC_WT decimal(9, 3) null
);


-- create table SERIALISED_RUN
-- (
--     RUN_ID  varchar(40) not null,
--     SER_OBJ binary(1)   not null
-- );
--
--
-- create table SERIALISED_WORKFLOW
-- (
--     WORKFLOW_ID varchar(40) not null,
--     SER_OBJ     binary(1)   not null
-- );


create table SHIFT_DATA
(
    RUN_ID         varchar(40) not null,
    YEAR           decimal(4)  not null,
    MONTH          decimal(2)  not null,
    DATA_SOURCE_ID decimal     not null,
    PORTROUTE      decimal(4)  not null,
    WEEKDAY        decimal(1)  not null,
    ARRIVEDEPART   decimal(1)  not null,
    TOTAL          decimal     not null,
    AM_PM_NIGHT    decimal(1)  not null
);

create index SHIFT_DATA_RUN_ID_index
    on SHIFT_DATA (RUN_ID);


-- create table SPSS_METADATA
-- (
--     NAME   varchar(30) null,
--     TYPE   varchar(30) null,
--     LENGTH decimal(3)  null
-- );


-- create table SQL_QUERY
-- (
--     TASK_ID       varchar(40)   not null,
--     QUERY_STRING  text          not null,
--     QUERY_MESSAGE varchar(4000) null
-- );


-- create table STATE_MAINTENANCE
-- (
--     STATE_ID    decimal      not null,
--     USER_ID     varchar(20)  null,
--     WORKFLOW_ID decimal      not null,
--     ACTION      varchar(30)  null,
--     OBJECT      varchar(100) null,
--     STATUS      decimal(1)   null,
--     COMMENTS    varchar(500) null
-- );


-- create table STEP
-- (
--     STEP_ID         varchar(40)   not null,
--     STEP_DEFINITION text          not null,
--     STEP_MESSAGE    varchar(4000) null
-- );


-- create table SURVEY_COLUMN
-- (
--     VERSION_ID    decimal     not null,
--     COLUMN_NO     decimal(4)  not null,
--     COLUMN_DESC   varchar(30) not null,
--     COLUMN_TYPE   varchar(20) not null,
--     COLUMN_LENGTH decimal(5)  not null
-- );


create table SURVEY_SUBSAMPLE
(
    RUN_ID                 varchar(40)    not null,
    SERIAL                 decimal(15)    not null,
    AGE                    decimal(3)     null,
    AM_PM_NIGHT            decimal(1)     null,
    ANYUNDER16             varchar(2)     null,
    APORTLATDEG            decimal(2)     null,
    APORTLATMIN            decimal(2)     null,
    APORTLATSEC            decimal(2)     null,
    APORTLATNS             varchar(1)     null,
    APORTLONDEG            decimal(3)     null,
    APORTLONMIN            decimal(2)     null,
    APORTLONSEC            decimal(2)     null,
    APORTLONEW             varchar(1)     null,
    ARRIVEDEPART           decimal(1)     null,
    BABYFARE               decimal(4, 2)  null,
    BEFAF                  decimal(6)     null,
    CHANGECODE             decimal(6)     null,
    CHILDFARE              decimal(4, 2)  null,
    COUNTRYVISIT           decimal(4)     null,
    CPORTLATDEG            decimal(2)     null,
    CPORTLATMIN            decimal(2)     null,
    CPORTLATSEC            decimal(2)     null,
    CPORTLATNS             varchar(1)     null,
    CPORTLONDEG            decimal(3)     null,
    CPORTLONMIN            decimal(2)     null,
    CPORTLONSEC            decimal(2)     null,
    CPORTLONEW             varchar(3)     null,
    INTDATE                varchar(8)     null,
    DAYTYPE                decimal(1)     null,
    DIRECTLEG              decimal(6)     null,
    DVEXPEND               decimal(6)     null,
    DVFARE                 decimal(6)     null,
    DVLINECODE             decimal(6)     null,
    DVPACKAGE              decimal(1)     null,
    PACKAGECOST            float          null,
    DVPACKCOST             decimal(6)     null,
    DVPERSONS              decimal(3)     null,
    DVPORTCODE             decimal(6)     null,
    EXPENDCODE             varchar(4)     null,
    EXPENDITURE            decimal(6)     null,
    FARE                   decimal(6)     null,
    FAREK                  decimal(2)     null,
    FLOW                   decimal(2)     null,
    HAULKEY                decimal(2)     null,
    INTENDLOS              decimal(2)     null,
    KIDAGE                 decimal(2)     null,
    LOSKEY                 decimal(2)     null,
    MAINCONTRA             decimal(1)     null,
    MIGSI                  int            null,
    INTMONTH               decimal(2)     null,
    NATIONALITY            decimal(4)     null,
    NATIONNAME             varchar(50)    null,
    NIGHTS1                decimal(3)     null,
    NIGHTS2                decimal(3)     null,
    NIGHTS3                decimal(3)     null,
    NIGHTS4                decimal(3)     null,
    NIGHTS5                decimal(3)     null,
    NIGHTS6                decimal(3)     null,
    NIGHTS7                decimal(3)     null,
    NIGHTS8                decimal(3)     null,
    NUMADULTS              decimal(3)     null,
    NUMDAYS                decimal(3)     null,
    NUMNIGHTS              decimal(3)     null,
    NUMPEOPLE              decimal(3)     null,
    PACKAGEHOL             decimal(1)     null,
    PACKAGEHOLUK           decimal(1)     null,
    PERSONS                decimal(2)     null,
    PORTROUTE              decimal(4)     null,
    PACKAGE                decimal(2)     null,
    PROUTELATDEG           decimal(2)     null,
    PROUTELATMIN           decimal(2)     null,
    PROUTELATSEC           decimal(2)     null,
    PROUTELATNS            varchar(1)     null,
    PROUTELONDEG           decimal(3)     null,
    PROUTELONMIN           decimal(2)     null,
    PROUTELONSEC           decimal(2)     null,
    PROUTELONEW            varchar(1)     null,
    PURPOSE                decimal(2)     null,
    QUARTER                decimal(1)     null,
    RESIDENCE              decimal(4)     null,
    RESPNSE                decimal(2)     null,
    SEX                    decimal(1)     null,
    SHIFTNO                decimal(6)     null,
    SHUTTLE                decimal(1)     null,
    SINGLERETURN           decimal(1)     null,
    TANDTSI                decimal(8)     null,
    TICKETCOST             decimal(6)     null,
    TOWNCODE1              decimal(6)     null,
    TOWNCODE2              decimal(6)     null,
    TOWNCODE3              decimal(6)     null,
    TOWNCODE4              decimal(6)     null,
    TOWNCODE5              decimal(6)     null,
    TOWNCODE6              decimal(6)     null,
    TOWNCODE7              decimal(6)     null,
    TOWNCODE8              decimal(6)     null,
    TRANSFER               decimal(6)     null,
    UKFOREIGN              decimal(1)     null,
    VEHICLE                decimal(1)     null,
    VISITBEGAN             varchar(8)     null,
    WELSHNIGHTS            decimal(3)     null,
    WELSHTOWN              decimal(6)     null,
    AM_PM_NIGHT_PV         decimal(1)     null,
    APD_PV                 decimal(4)     null,
    ARRIVEDEPART_PV        decimal(1)     null,
    CROSSINGS_FLAG_PV      decimal(1)     null,
    STAYIMPCTRYLEVEL1_PV   decimal(8)     null,
    STAYIMPCTRYLEVEL2_PV   decimal(8)     null,
    STAYIMPCTRYLEVEL3_PV   decimal(8)     null,
    STAYIMPCTRYLEVEL4_PV   decimal(8)     null,
    DAY_PV                 decimal(2)     null,
    DISCNT_F1_PV           decimal(4, 3)  null,
    DISCNT_F2_PV           decimal(4, 3)  null,
    DISCNT_PACKAGE_COST_PV decimal(6)     null,
    DUR1_PV                decimal(3)     null,
    DUR2_PV                decimal(3)     null,
    DUTY_FREE_PV           decimal(4)     null,
    FAGE_PV                decimal(2)     null,
    FARES_IMP_ELIGIBLE_PV  decimal(1)     null,
    FARES_IMP_FLAG_PV      decimal(1)     null,
    FLOW_PV                decimal(2)     null,
    FOOT_OR_VEHICLE_PV     decimal(2)     null,
    HAUL_PV                varchar(2)     null,
    IMBAL_CTRY_FACT_PV     decimal(5, 3)  null,
    IMBAL_CTRY_GRP_PV      decimal(3)     null,
    IMBAL_ELIGIBLE_PV      decimal(1)     null,
    IMBAL_PORT_FACT_PV     decimal(5, 3)  null,
    IMBAL_PORT_GRP_PV      decimal(3)     null,
    IMBAL_PORT_SUBGRP_PV   decimal(3)     null,
    LOS_PV                 decimal(3)     null,
    LOSDAYS_PV             decimal(3)     null,
    MIG_FLAG_PV            decimal(1)     null,
    MINS_CTRY_GRP_PV       decimal(6)     null,
    MINS_CTRY_PORT_GRP_PV  varchar(10)    null,
    MINS_FLAG_PV           decimal(1)     null,
    MINS_NAT_GRP_PV        decimal(6)     null,
    MINS_PORT_GRP_PV       decimal(3)     null,
    MINS_QUALITY_PV        decimal(1)     null,
    NR_FLAG_PV             decimal(1)     null,
    NR_PORT_GRP_PV         decimal(3)     null,
    OPERA_PV               decimal(2)     null,
    OSPORT1_PV             decimal(8)     null,
    OSPORT2_PV             decimal(8)     null,
    OSPORT3_PV             decimal(8)     null,
    OSPORT4_PV             decimal(8)     null,
    PUR1_PV                decimal(8)     null,
    PUR2_PV                decimal(8)     null,
    PUR3_PV                decimal(8)     null,
    PURPOSE_PV             decimal(8)     null,
    QMFARE_PV              decimal(8)     null,
    RAIL_CNTRY_GRP_PV      decimal(3)     null,
    RAIL_EXERCISE_PV       decimal(6)     null,
    RAIL_IMP_ELIGIBLE_PV   decimal(1)     null,
    REG_IMP_ELIGIBLE_PV    decimal(1)     null,
    SAMP_PORT_GRP_PV       varchar(10)    null,
    SHIFT_FLAG_PV          decimal(1)     null,
    SHIFT_PORT_GRP_PV      varchar(10)    null,
    SPEND_IMP_FLAG_PV      decimal(1)     null,
    SPEND_IMP_ELIGIBLE_PV  decimal(1)     null,
    STAY_IMP_ELIGIBLE_PV   decimal(1)     null,
    STAY_IMP_FLAG_PV       decimal(1)     null,
    STAY_PURPOSE_GRP_PV    decimal(2)     null,
    TOWNCODE_PV            varchar(10)    null,
    TOWN_IMP_ELIGIBLE_PV   decimal(1)     null,
    TYPE_PV                decimal(2)     null,
    UK_OS_PV               decimal(1)     null,
    UKPORT1_PV             decimal(8)     null,
    UKPORT2_PV             decimal(8)     null,
    UKPORT3_PV             decimal(8)     null,
    UKPORT4_PV             decimal(8)     null,
    UNSAMP_PORT_GRP_PV     varchar(10)    null,
    UNSAMP_REGION_GRP_PV   decimal(9, 3)  null,
    WEEKDAY_END_PV         decimal(1)     null,
    DIRECT                 decimal(6)     null,
    EXPENDITURE_WT         decimal(6, 3)  null,
    EXPENDITURE_WTK        float    null,
    FAREKEY                varchar(4)     null,
    OVLEG                  decimal(6)     null,
    SPEND                  decimal(7)     null,
    SPEND1                 decimal(7)     null,
    SPEND2                 decimal(7)     null,
    SPEND3                 decimal(7)     null,
    SPEND4                 decimal(7)     null,
    SPEND5                 decimal(7)     null,
    SPEND6                 decimal(7)     null,
    SPEND7                 decimal(7)     null,
    SPEND8                 decimal(7)     null,
    SPEND9                 decimal(7)     null,
    SPENDIMPREASON         decimal(1)     null,
    SPENDK                 decimal(2)     null,
    STAY                   decimal(3)     null,
    STAYK                  decimal(1)     null,
    STAY1K                 varchar(10)    null,
    STAY2K                 varchar(10)    null,
    STAY3K                 varchar(10)    null,
    STAY4K                 varchar(10)    null,
    STAY5K                 varchar(10)    null,
    STAY6K                 varchar(10)    null,
    STAY7K                 varchar(10)    null,
    STAY8K                 varchar(10)    null,
    STAY9K                 varchar(10)    null,
    STAYTLY                decimal(6)     null,
    STAY_WT                decimal(6, 3)  null,
    STAY_WTK               float    null,
    TYPEINTERVIEW          decimal(3)     null,
    UKLEG                  decimal(6)     null,
    VISIT_WT               decimal(6, 3)  null,
    VISIT_WTK              float    null,
    SHIFT_WT               decimal(9, 3)  null,
    NON_RESPONSE_WT        decimal(9, 3)  null,
    MINS_WT                decimal(9, 3)  null,
    TRAFFIC_WT             decimal(9, 3)  null,
    UNSAMP_TRAFFIC_WT      decimal(9, 3)  null,
    IMBAL_WT               decimal(9, 3)  null,
    FINAL_WT               decimal(12, 3) null
);

create index SURVEY_SUBSAMPLE_RUN_ID_index
    on SURVEY_SUBSAMPLE (RUN_ID);


-- create table SURVEY_VALUE
-- (
--     VERSION_ID   decimal      not null,
--     SERIAL_NO    decimal(15)  not null,
--     COLUMN_NO    decimal(4)   not null,
--     COLUMN_VALUE varchar(100) not null
-- );


-- create table TASK
-- (
--     TASK_ID      varchar(40) not null,
--     PARENT_ID    varchar(40) null,
--     SERVICE_NAME varchar(30) null,
--     TASK_NAME    varchar(30) not null,
--     DATE_CREATED date        not null,
--     TASK_STATUS  decimal(2)  not null
-- );
--
--
-- create table TASK_CHILD
-- (
--     TASK_ID  varchar(40) not null,
--     CHILD_ID varchar(40) not null
-- );
--
--
-- create table TASK_NODE
-- (
--     TASK_ID      varchar(40) not null,
--     PARENT_ID    varchar(40) null,
--     CHILD_ID     varchar(40) null,
--     TASK_STATUS  decimal(2)  not null,
--     DATE_CREATED date        not null
-- );
--
--
-- create table TASK_SAS_MAP
-- (
--     TASK_ID          varchar(40) not null,
--     PARAMETER_SET_ID decimal     not null
-- );


create table TRAFFIC_DATA
(
    RUN_ID         varchar(40)    not null,
    YEAR           decimal(4)     not null,
    MONTH          decimal(2)     not null,
    DATA_SOURCE_ID decimal        not null,
    PORTROUTE      decimal(4)     null,
    ARRIVEDEPART   decimal(1)     null,
    TRAFFICTOTAL   decimal(12, 3) not null,
    PERIODSTART    varchar(10)    null,
    PERIODEND      varchar(10)    null,
    AM_PM_NIGHT    decimal(1)     null,
    HAUL           varchar(2)     null,
    VEHICLE        decimal(1)     null
);


-- create table UD_SAS_OUTPUTS_VW
-- (
--     TASK_ID            varchar(160)  null,
--     STAT_ACT           varchar(400)  null,
--     STAT_UNIT          varchar(400)  null,
--     DATASET_TYPE       varchar(400)  null,
--     PERIOD_TYPE        text          null,
--     PERIOD_NAME        varchar(960)  null,
--     OUTPUT             varchar(960)  null,
--     FILE_NAME          varchar(1924) null,
--     VIEW_NAME          varchar(120)  null,
--     OVERFLOW_VIEW_NAME varchar(120)  null
-- );
--
--
-- create table UD_VAR_METADATA_VW
-- (
--     VAR_NAME            varchar(960) null,
--     VAR_SAS_FORMAT_NAME varchar(960) null,
--     VAR_VALID_FROM_DATE date         null,
--     VAR_DATA_TYPE       varchar(424) null,
--     VAR_DATA_LENGTH     decimal(38)  null,
--     VAR_DATA_PRECISION  decimal(38)  null,
--     VAR_DESCRIPTION     text         null,
--     VAR_TYPE_FLAG       varchar(4)   null,
--     VAR_LABEL           varchar(960) null,
--     VAR_LABEL_VALUE     varchar(960) null
-- );


create table UNSAMPLED_OOH_DATA
(
    RUN_ID         varchar(40)    not null,
    YEAR           decimal(4)     not null,
    MONTH          decimal(2)     not null,
    DATA_SOURCE_ID decimal        not null,
    PORTROUTE      decimal(4)     null,
    REGION         decimal(3)     null,
    ARRIVEDEPART   decimal(1)     null,
    UNSAMP_TOTAL   decimal(12, 3) not null
);


-- create table WORKFLOW
-- (
--     WORKFLOW_ID decimal     not null,
--     NAME        varchar(30) null,
--     PERIOD      varchar(6)  null
-- );
--
--
-- create table WORKFLOW_DEFINITION
-- (
--     WORKFLOW_DEF_ID decimal(3) not null,
--     WF_DEFINITION   text       not null
-- );


-- create table WORKSPACE_MAINTENANCE
-- (
--     WORKSPACE_ID decimal     not null,
--     WORKFLOW_ID  decimal     not null,
--     WORKSPACE    varchar(60) null,
--     STATE        varchar(20) null,
--     DATE_CREATED date        null
-- );


create table POPROWVEC_TRAFFIC
(
    `index` bigint null,
    C_GROUP bigint null,
    T_1     double null,
    T_2     double null,
    T_3     double null,
    T_4     double null,
    T_5     double null,
    T_6     double null,
    T_7     double null,
    T_8     double null,
    T_9     double null,
    T_10    double null,
    T_11    double null,
    T_12    double null,
    T_13    double null,
    T_14    double null,
    T_15    double null,
    T_16    double null,
    T_17    double null,
    T_18    double null,
    T_19    double null,
    T_20    double null,
    T_21    double null,
    T_22    double null,
    T_23    double null,
    T_24    double null,
    T_25    double null,
    T_26    double null,
    T_27    double null,
    T_28    double null,
    T_29    double null,
    T_30    double null,
    T_31    double null,
    T_32    double null,
    T_33    double null,
    T_34    double null,
    T_35    double null,
    T_36    double null,
    T_37    double null,
    T_38    double null,
    T_39    double null,
    T_40    double null,
    T_41    double null,
    T_42    double null,
    T_43    double null,
    T_44    double null,
    T_45    double null,
    T_46    double null,
    T_47    double null,
    T_48    double null,
    T_49    double null,
    T_50    double null,
    T_51    double null,
    T_52    double null,
    T_53    double null,
    T_54    double null,
    T_55    double null,
    T_56    double null,
    T_57    double null,
    T_58    double null,
    T_59    double null,
    T_60    double null,
    T_61    double null,
    T_62    double null,
    T_63    double null,
    T_64    double null,
    T_65    double null,
    T_66    double null,
    T_67    double null,
    T_68    double null,
    T_69    double null,
    T_70    double null,
    T_71    double null,
    T_72    double null,
    T_73    double null,
    T_74    double null,
    T_75    double null,
    T_76    double null,
    T_77    double null
);

create index ix_poprowvec_traffic_index
    on POPROWVEC_TRAFFIC (`index`);


create table POPROWVEC_UNSAMP
(
    C_GROUP bigint null,
    T_1     float  null,
    T_2     float  null,
    T_3     float  null,
    T_4     float  null,
    T_5     float  null,
    T_6     float  null,
    T_7     float  null,
    T_8     float  null,
    T_9     float  null,
    T_10    float  null,
    T_11    float  null,
    T_12    float  null,
    T_13    float  null,
    T_14    float  null,
    T_15    float  null,
    T_16    float  null,
    T_17    float  null,
    T_18    float  null,
    T_19    float  null,
    T_20    float  null,
    T_21    float  null,
    T_22    float  null,
    T_23    float  null,
    T_24    float  null,
    T_25    float  null,
    T_26    float  null,
    T_27    float  null,
    T_28    float  null,
    T_29    float  null,
    T_30    float  null,
    T_31    float  null,
    T_32    float  null,
    T_33    float  null,
    T_34    float  null,
    T_35    float  null,
    T_36    float  null,
    T_37    float  null,
    T_38    float  null,
    T_39    float  null,
    T_40    float  null,
    T_41    float  null,
    T_42    float  null,
    T_43    float  null,
    T_44    float  null,
    T_45    float  null,
    T_46    float  null,
    T_47    float  null,
    T_48    float  null,
    T_49    float  null,
    T_50    float  null,
    T_51    float  null,
    T_52    float  null,
    T_53    float  null,
    T_54    float  null,
    T_55    float  null,
    T_56    float  null,
    T_57    float  null,
    T_58    float  null,
    T_59    float  null,
    T_60    float  null,
    T_61    float  null,
    T_62    float  null,
    T_63    float  null,
    T_64    float  null,
    T_65    float  null,
    T_66    float  null,
    T_67    float  null,
    T_68    float  null,
    T_69    float  null,
    T_70    float  null,
    T_71    float  null,
    T_72    float  null,
    T_73    float  null,
    T_74    float  null,
    T_75    float  null,
    T_76    float  null,
    T_77    float  null,
    T_78    float  null,
    T_79    float  null,
    T_80    float  null,
    T_81    float  null,
    T_82    float  null,
    T_83    float  null,
    T_84    float  null,
    T_85    float  null,
    T_86    float  null,
    T_87    float  null,
    T_88    float  null,
    T_89    float  null,
    T_90    float  null,
    T_91    float  null,
    T_92    float  null,
    T_93    float  null,
    T_94    float  null,
    T_95    float  null,
    T_96    float  null,
    T_97    float  null,
    T_98    float  null,
    T_99    float  null,
    T_100   float  null,
    T_101   float  null,
    T_102   float  null,
    T_103   float  null,
    T_104   float  null,
    T_105   float  null,
    T_106   float  null,
    T_107   float  null,
    T_108   float  null,
    T_109   float  null,
    T_110   float  null,
    T_111   float  null,
    T_112   float  null,
    T_113   float  null,
    T_114   float  null,
    T_115   float  null,
    T_116   float  null,
    T_117   float  null,
    T_118   float  null,
    T_119   float  null,
    T_120   float  null,
    T_121   float  null,
    T_122   float  null,
    T_123   float  null,
    T_124   float  null,
    T_125   float  null,
    T_126   float  null,
    T_127   float  null,
    T_128   float  null,
    T_129   float  null,
    T_130   float  null,
    T_131   float  null,
    T_132   float  null,
    T_133   float  null,
    T_134   float  null,
    T_135   float  null,
    T_136   float  null,
    T_137   float  null,
    T_138   float  null,
    T_139   float  null,
    T_140   float  null,
    T_141   float  null,
    T_142   float  null,
    T_143   float  null,
    T_144   float  null,
    T_145   float  null,
    T_146   float  null,
    T_147   float  null,
    T_148   float  null,
    T_149   float  null,
    T_150   float  null,
    T_151   float  null,
    T_152   float  null,
    T_153   float  null,
    T_154   float  null,
    T_155   float  null,
    T_156   float  null,
    T_157   float  null,
    T_158   float  null,
    T_159   float  null,
    T_160   float  null,
    T_161   float  null,
    T_162   float  null,
    T_163   float  null,
    T_164   float  null,
    T_165   float  null,
    T_166   float  null,
    T_167   float  null,
    T_168   float  null,
    T_169   float  null,
    T_170   float  null,
    T_171   float  null,
    T_172   float  null,
    T_173   float  null,
    T_174   float  null,
    T_175   float  null,
    T_176   float  null,
    T_177   float  null,
    T_178   float  null,
    T_179   float  null,
    T_180   float  null,
    T_181   float  null,
    T_182   float  null,
    T_183   float  null,
    T_184   float  null,
    T_185   float  null,
    T_186   float  null,
    T_187   float  null,
    T_188   float  null,
    T_189   float  null,
    T_190   float  null,
    T_191   float  null,
    T_192   float  null,
    T_193   float  null,
    T_194   float  null,
    T_195   float  null,
    T_196   float  null,
    T_197   float  null,
    T_198   float  null,
    T_199   float  null,
    T_200   float  null,
    T_201   float  null,
    T_202   float  null,
    T_203   float  null
);

create index IX_POPROWVEC_UNSAMP_C_GROUP
    on POPROWVEC_UNSAMP (C_GROUP);


create table R_TRAFFIC
(
    rownames           varchar(255) null,
    SERIAL             decimal(15)  null,
    ARRIVEDEPART       int          null,
    PORTROUTE          int          null,
    SAMP_PORT_GRP_PV   varchar(255) null,
    SHIFT_WT           float        null,
    NON_RESPONSE_WT    double       null,
    MINS_WT            double       null,
    TRAFFIC_WT         double       null,
    TRAF_DESIGN_WEIGHT double       null,
    T1                 int          null,
    T_                 varchar(255) null,
    TW_WEIGHT          double       null
);


create table R_UNSAMPLED
(
    rownames              varchar(255)  null,
    SERIAL                decimal(15)   null,
    ARRIVEDEPART          int           null,
    PORTROUTE             int           null,
    UNSAMP_PORT_GRP_PV    varchar(255)  null,
    UNSAMP_REGION_GRP_PV  decimal(9, 3) null,
    SHIFT_WT              float         null,
    NON_RESPONSE_WT       float         null,
    MINS_WT               float         null,
    UNSAMP_TRAFFIC_WT     decimal(9, 3) null,
    OOH_DESIGN_WEIGHT     float         null,
    T1                    int           null,
    T_                    varchar(255)  null,
    UNSAMP_TRAFFIC_WEIGHT float         null
);


-- create table sqlResult
-- (
--     rownames       varchar(255) null,
--     RUN_ID         varchar(255) null,
--     YEAR           int          null,
--     MONTH          int          null,
--     DATA_SOURCE_ID int          null,
--     PORTROUTE      int          null,
--     ARRIVEDEPART   int          null,
--     TRAFFICTOTAL   float        null,
--     PERIODSTART    int          null,
--     PERIODEND      int          null,
--     AM_PM_NIGHT    int          null,
--     HAUL           varchar(255) null,
--     VEHICLE        int          null
-- );


create table SURVEY_TRAFFIC_AUX
(
    SERIAL             decimal(15)  null,
    ARRIVEDEPART       int          null,
    PORTROUTE          int          null,
    SAMP_PORT_GRP_PV   varchar(255) null,
    SHIFT_WT           float        null,
    NON_RESPONSE_WT    float        null,
    MINS_WT            float        null,
    TRAFFIC_WT         varchar(5)   null,
    TRAF_DESIGN_WEIGHT float        null,
    T1                 int          null
);


create table SURVEY_UNSAMP_AUX
(
    SERIAL               decimal(15)   null,
    ARRIVEDEPART         int           null,
    PORTROUTE            int           null,
    UNSAMP_PORT_GRP_PV   varchar(255)  null,
    UNSAMP_REGION_GRP_PV decimal(9, 3) null,
    SHIFT_WT             float         null,
    NON_RESPONSE_WT      float         null,
    MINS_WT              float         null,
    UNSAMP_TRAFFIC_WT    decimal(9, 3) null,
    OOH_DESIGN_WEIGHT    float         null,
    T1                   int           null
);

INSERT INTO ips.RUN
(RUN_ID, RUN_NAME, RUN_DESC, USER_ID, PERIOD, YEAR, RUN_STATUS, RUN_TYPE_ID, LAST_MODIFIED)
VALUES ('b63786be-25b1-4f30-bfd9-a240a10f0ede', 'TM_Create_and_Edit_Test',
        'Test run to check run creation and editing are still working after changing the fieldwork selection options',
        'smptester', "01", 2019, 0, 0, CURDATE()),
       ('0eada784-7caf-4f68-b26f-4699c9bf0032', 'TestRun1', 'Demo test run', 'smptester', "02", 2018, 3, 0, CURDATE()),
       ('09e5c1872-3f8e-4ae5-85dc-c67a602d011e', 'IPS_Test_Run_December_2017',
        'IPS run that contains data for the December period of 2017. This is our demo run, skip the dataimport step.',
        'smptester', "12", 2017, 2, 1, CURDATE()),
       ('6aee5893-79bd-4e6b-923e-c41a9d3b56d9', 'IPS_Run December 2017', 'Demo Run', 'smptester', "11", 2017, 3, 0,
        CURDATE()),
       ('b33e6aa9-415a-408f-a871-04701fadbd70', 'HandoverRun', 'Test run for handover demo.', 'mahont1', "01", 2019, 0,
        0, CURDATE());

CREATE TABLE G_PVs
(
    PV_ID int(11)      NOT NULL,
    Name  varchar(255) NOT NULL,
    PRIMARY KEY (PV_ID)
);

INSERT INTO G_PVs(PV_ID, Name)
VALUES (12, 'imbal_port_fact_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (13, 'stay_imp_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (14, 'stay_imp_eligible_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (33, 'osport2_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (47, 'rail_cntry_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (34, 'osport3_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (35, 'osport4_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (36, 'apd_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (37, 'qmfare_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (38, 'duty_free_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (39, 'spend_imp_eligible_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (40, 'uk_os_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (41, 'pur1_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (42, 'pur2_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (43, 'pur3_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (44, 'dur1_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (45, 'dur2_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (46, 'imbal_ctry_fact_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (15, 'StayImpCtryLevel1_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (16, 'StayImpCtryLevel2_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (28, 'ukport1_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (29, 'ukport2_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (30, 'ukport3_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (31, 'ukport4_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (32, 'osport1_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (48, 'rail_exercise_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (49, 'rail_imp_eligible_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (50, 'spend_imp_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (51, 'purpose_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (52, 'town_imp_eligible_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (53, 'reg_imp_eligible_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (54, 'mins_ctry_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (55, 'mins_port_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (56, 'samp_port_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (57, 'unsamp_port_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (4, 'shift_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (5, 'crossings_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (6, 'shift_port_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (7, 'nr_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (8, 'nr_port_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (9, 'mins_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (10, 'imbal_eligible_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (11, 'imbal_port_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (58, 'unsamp_region_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (1, 'weekday_end_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (2, 'am_pm_night_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (3, 'mig_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (17, 'StayImpCtryLevel3_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (18, 'StayImpCtryLevel4_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (19, 'stay_purpose_grp_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (20, 'fares_imp_flag_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (21, 'fares_imp_eligible_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (22, 'discnt_f1_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (23, 'discnt_package_cost_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (24, 'discnt_f2_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (25, 'fage_pv');
INSERT INTO G_PVs(PV_ID, Name)
VALUES (26, 'type_pv');

CREATE TABLE `G_PV_Variables`
(
    `PV_Variable_ID` int(11)      NOT NULL,
    `PV_ID`          int(11)      NOT NULL,
    `Name`           varchar(255) NOT NULL,
    PRIMARY KEY (`PV_Variable_ID`),
    KEY `FK__G_PV_Vari__PV_ID__2739D489` (`PV_ID`),
    CONSTRAINT `FK__G_PV_Vari__PV_ID__2739D489` FOREIGN KEY (`PV_ID`) REFERENCES `G_PVs` (`PV_ID`) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (1, 12, 'row["IMBAL_PORT_FACT_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (2, 12, 'row["IMBAL_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (3, 12, 'row["ARRIVEDEPART"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (4, 13, 'row["STAY_IMP_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (5, 13, 'row["NUMNIGHTS"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (6, 14, 'row["STAY_IMP_ELIGIBLE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (7, 14, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (8, 14, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (9, 14, 'row["MINS_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (10, 33, 'row["RESIDENCE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (11, 33, 'row["UKFOREIGN"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (12, 33, 'row["UKPORT1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (13, 33, 'row["OSPORT2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (14, 33, 'row["COUNTRYVISIT"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (15, 33, 'row["OSPORT1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (16, 47, 'row["RAIL_CNTRY_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (17, 47, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (18, 47, 'row["RESIDENCE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (19, 47, 'row["COUNTRYVISIT"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (20, 47, 'railcountry');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (21, 34, 'row["OSPORT3_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (22, 34, 'row["OSPORT2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (23, 35, 'row["OSPORT3_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (24, 35, 'row["OSPORT4_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (25, 36, 'row["APD_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (26, 36, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (27, 36, 'row["OSPORT2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (28, 36, 'APDBAND');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (29, 37, 'row["MINS_FLAG_PV"])');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (30, 37, 'row["QMFARE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (31, 37, 'row["OSPORT3_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (32, 37, 'row["MINS_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (33, 38, 'row["PURPOSE"])');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (34, 38, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (35, 38, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (36, 38, 'row["DUTY_FREE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (37, 39, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (38, 39, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (39, 39, 'row["SPEND_IMP_ELIGIBLE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (40, 39, 'row["MINS_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (41, 40, 'row["UK_OS_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (42, 40, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (43, 41, 'row["DVPACKAGE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (44, 41, 'row["IND"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (45, 41, 'row["PUR1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (46, 41, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (47, 42, 'row["IND"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (48, 42, 'row["PUR2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (49, 42, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (50, 43, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (51, 43, 'row["PUR3_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (52, 44, 'row["DUR1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (53, 44, 'row["STAY"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (54, 45, 'row["DUR2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (55, 45, 'row["STAY"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (56, 46, 'row["RESIDENCE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (57, 46, 'row["IMBAL_CTRY_FACT_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (58, 15, 'row["STAYIMPCTRYLEVEL1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (59, 15, 'row["COUNTRYVISIT"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (60, 15, 'row["RESIDENCE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (61, 15, 'row["UKFOREIGN"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (62, 16, 'row["STAYIMPCTRYLEVEL1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (63, 16, 'row["STAYIMPCTRYLEVEL2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (64, 28, 'row["PORTROUTE"]</PV_DEF>');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (65, 28, 'row["UKPORT1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (66, 28, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (67, 29, 'row["UKPORT2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (68, 29, 'row["UKPORT1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (69, 30, 'row["UKPORT2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (70, 30, 'row["UKPORT3_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (71, 31, 'row["UKPORT4_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (72, 31, 'row["UKPORT3_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (73, 32, 'row["DVPORTCODE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (74, 32, 'row["OSPORT1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (75, 32, 'row["CHANGECODE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (76, 48, 'row["RAIL_CNTRY_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (77, 48, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (78, 48, 'row["RAIL_EXERCISE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (79, 49, 'row["RAIL_IMP_ELIGIBLE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (80, 49, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (81, 50, 'row["SPEND_IMP_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (82, 50, 'row["SPEND"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (83, 51, 'row["PURPOSE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (84, 51, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (85, 52, 'row["RESPNSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (86, 52, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (87, 52, 'row["TOWN_IMP_ELIGIBLE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (88, 52, 'row["PURPOSE"])');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (89, 52, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (90, 53, 'row["RESPNSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (91, 53, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (92, 53, 'row["REG_IMP_ELIGIBLE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (93, 53, 'row["PURPOSE"])');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (94, 53, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (95, 54, '<PV_DEF>row["MINS_CTRY_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (96, 54, 'row["FLOW"]</PV_DEF>');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (97, 55, 'row["MINS_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (98, 55, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (99, 56, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (100, 56, 'row["RESIDENCE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (101, 56, 'row["HAUL"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (102, 56, 'row["DVPORTCODE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (103, 56, 'Irish');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (104, 56, 'IoM');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (105, 56, 'row["ARRIVEDEPART"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (106, 56, 'row["SAMP_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (107, 56, 'ChannelI');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (108, 56, 'dvpc');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (109, 56, 'row["COUNTRYVISIT"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (110, 56, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (111, 57, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (112, 57, 'row["RESIDENCE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (113, 57, 'row["DVPORTCODE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (114, 57, 'row["UNSAMP_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (115, 57, 'Irish');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (116, 57, 'IoM');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (117, 57, 'ChannelI');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (118, 57, 'dvpc');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (119, 57, 'row["COUNTRYVISIT"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (120, 57, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (121, 4, 'row["SHIFT_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (122, 4, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (123, 5, 'row["CROSSINGS_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (124, 5, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (125, 6, 'row["SHIFT_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (126, 6, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (127, 7, 'row["RESPNSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (128, 7, 'row["NR_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (129, 8, '<PV_DEF>row["NR_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (130, 9, 'row["TYPEINTERVIEW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (131, 9, 'row["RESPNSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (132, 9, 'row["MINS_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (133, 10, 'row["RESPNSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (134, 10, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (135, 10, 'row["IMBAL_ELIGIBLE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (136, 10, 'row["INTENDLOS"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (137, 10, 'row["PURPOSE"]))');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (138, 10, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (139, 11, 'row["IMBAL_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (140, 11, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (141, 58, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (142, 58, 'row["RESIDENCE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (143, 58, 'row["UNSAMP_REGION_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (144, 58, 'row["DVPORTCODE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (145, 58, 'row["UNSAMP_PORT_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (146, 58, 'row["ARRIVEDEPART"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (147, 58, 'dvpc');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (148, 58, 'row["COUNTRYVISIT"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (149, 58, 'row["REGION"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (150, 58, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (151, 1, 'd');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (152, 1, 'row["INTDATE"][:2]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (153, 1, 'row["INTDATE"][2:4]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (154, 1, 'year');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (155, 1, 'weekday');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (156, 1, 'month');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (157, 1, 'dayweek');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (158, 1, 'row["INTDATE"][4:8]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (159, 1, 'row["WEEKDAY_END_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (160, 1, 'row["WEEKDAY"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (161, 1, 'day');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (162, 1, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (163, 2, 'row["AM_PM_NIGHT_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (164, 2, 'row["PORTROUTE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (165, 2, 'row["AM_PM_NIGHT"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (166, 3, 'row["LOSKEY"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (167, 3, 'row["MIG_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (168, 17, 'row["STAYIMPCTRYLEVEL2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (169, 17, 'row["STAYIMPCTRYLEVEL3_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (170, 18, 'row["STAYIMPCTRYLEVEL2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (171, 18, 'row["STAYIMPCTRYLEVEL4_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (172, 19, 'row["PURPOSE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (173, 19, 'row["STAY_PURPOSE_GRP_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (174, 20, 'row["FARES_IMP_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (175, 20, 'row["DVFARE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (176, 21, 'row["FARES_IMP_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (177, 21, 'row["FARES_IMP_ELIGIBLE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (178, 21, 'row["MINS_FLAG_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (179, 21, 'row["FAREKEY"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (180, 22, 'row["DISCNT_F1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (181, 22, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (182, 23, 'row["DISCNT_F1_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (183, 23, 'row["DISCNT_PACKAGE_COST_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (184, 23, 'row["DISCNT_PACKAGE_COST_PV"],');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (185, 23, 'row["PACKAGE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (186, 24, 'row["FLOW"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (187, 24, 'row["DISCNT_F2_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (188, 24, 'row["PACKAGE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (189, 25, 'row["KIDAGE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (190, 25, 'row["AGE"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (191, 25, 'row["FAGE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (192, 26, 'row["TYPE_PV"]');
INSERT INTO G_PV_Variables(PV_Variable_ID, PV_ID, Name)
VALUES (193, 26, 'row["PURPOSE"]');

CREATE TABLE PV_Block
(
    Block_ID    int(11)      NOT NULL AUTO_INCREMENT,
    Run_ID      varchar(255) NOT NULL,
    Block_Index int(11)      NOT NULL,
    PV_ID       int(11)      NOT NULL,
    PRIMARY KEY (Block_ID),
    KEY pv_fk (PV_ID),
    CONSTRAINT pv_fk FOREIGN KEY (PV_ID) REFERENCES G_PVs (PV_ID) ON DELETE CASCADE ON UPDATE CASCADE
);


CREATE TABLE PV_Expression
(
    Expression_ID    int(11) NOT NULL AUTO_INCREMENT,
    Block_ID         int(11) NOT NULL,
    Expression_Index int(11) NOT NULL,
    PRIMARY KEY (Expression_ID),
    KEY block_fk (Block_ID),
    CONSTRAINT block_fk FOREIGN KEY (Block_ID) REFERENCES PV_Block (Block_ID) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE PV_Element
(
    Element_ID    int(11)      NOT NULL AUTO_INCREMENT,
    Expression_ID int(11)      NOT NULL,
    type          varchar(255) NOT NULL,
    content       varchar(255) NOT NULL,
    PRIMARY KEY (Element_ID),
    KEY expression_fk (Expression_ID),
    CONSTRAINT expression_fk FOREIGN KEY (Expression_ID) REFERENCES PV_Expression (Expression_ID) ON DELETE CASCADE ON UPDATE CASCADE
);

create table USER
(
    ID         int auto_increment primary key,
    USER_NAME  varchar(80)  null,
    PASSWORD   varchar(255) null,
    FIRST_NAME varchar(255) null,
    SURNAME    varchar(255) null,
    ROLE       varchar(50)  null
);

INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (1, 'Admin', 'pbkdf2:sha256:50000$jYlAjFyT$a3990f67a04492fdffae29256cc168caf7becbe33ca6fefb2f89c04b00ef9d27',
        null, null, 'admin');

SET FOREIGN_KEY_CHECKS = 1;
