CREATE DATABASE IF NOT EXISTS ips;

grant all on ips.* to 'ips'@'%' with grant option;

use ips;

create table EXPORT_DATA_DOWNLOAD
(
  RUN_ID            varchar(40) not null,
  DOWNLOADABLE_DATA longtext    null,
  FILENAME          varchar(40) null,
  SOURCE_TABLE      varchar(40) null,
  DATE_CREATED      text        null
);


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


create table PROCESS_VARIABLE_PY
(
  RUN_ID              varchar(40)   not null,
  PROCESS_VARIABLE_ID decimal       not null,
  PV_NAME             varchar(30)   not null,
  PV_DESC             varchar(1000) not null,
  PV_DEF              text          not null
);


create table PROCESS_VARIABLE_SET
(
  RUN_ID varchar(40) not null,
  NAME   varchar(30) not null,
  USER   varchar(50) null,
  PERIOD varchar(12) not null,
  YEAR   year(4)     not null
);


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


create table RESPONSE
(
  RUN_ID        varchar(40)                        not null,
  STEP_NUMBER   int                                not null,
  RESPONSE_CODE int                                not null,
  MESSAGE       varchar(250)                       null,
  TIME_STAMP    datetime default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP
);


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


create table RUN_STEPS
(
  RUN_ID      varchar(40) not null,
  STEP_NUMBER decimal(2)  not null,
  STEP_NAME   varchar(80) not null,
  STEP_STATUS decimal(2)  not null
);


create table SAS_AIR_MILES
(
  SERIAL    decimal(15) not null,
  DIRECTLEG decimal(6)  null,
  OVLEG     decimal(6)  null,
  UKLEG     decimal(6)  null
);


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
  VISIT_WTK       float         null,
  STAY_WTK        float         null,
  EXPENDITURE_WTK float         null,
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
  EXPENDITURE_WTK        float          null,
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
  STAY_WTK               float          null,
  TYPEINTERVIEW          decimal(3)     null,
  UKLEG                  decimal(6)     null,
  VISIT_WT               decimal(6, 3)  null,
  VISIT_WTK              float          null,
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
  EXPENDITURE_WTK        float          null,
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
  STAY_WTK               float          null,
  TYPEINTERVIEW          decimal(3)     null,
  UKLEG                  decimal(6)     null,
  VISIT_WT               decimal(6, 3)  null,
  VISIT_WTK              float          null,
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


create index SURVEY_SUBSAMPLE_SERIAL_index
  on SURVEY_SUBSAMPLE (SERIAL);


create index SURVEY_SUBSAMPLE_RESPNSE_index
  on SURVEY_SUBSAMPLE (RESPNSE);


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


create table USER
(
  ID         int auto_increment primary key,
  USER_NAME  varchar(80)  null,
  PASSWORD   varchar(255) null,
  FIRST_NAME varchar(255) null,
  SURNAME    varchar(255) null,
  ROLE       varchar(50)  null
);


SET SQL_SAFE_UPDATES = 0;


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 1, 'weekday_end_pv', 'weekday_end_pv               ',
        'if dataset == ''survey'':\n    weekday = float(''nan'')\n\n    day = int(row[''INTDATE''][:2])\n    month = int(row[''INTDATE''][2:4])\n    year = int(row[''INTDATE''][4:8])\n\n    d = datetime(year,month,day)\n\n    dayweek = (d.isoweekday() + 1) % 7\n\n    if (row[''PORTROUTE''] == 811):\n        if (dayweek >= 2 and dayweek <= 5):\n            weekday = 1\n        else:\n            weekday = 2\n    else:\n        if (dayweek >= 2 and dayweek <= 6):\n            weekday = 1\n        else:\n            weekday = 2\n\n    if (row[''PORTROUTE''] == 811):\n        row[''WEEKDAY_END_PV''] = weekday\n    elif (row[''PORTROUTE''] >= 600):\n        row[''WEEKDAY_END_PV''] = 1\n    else:\n        row[''WEEKDAY_END_PV''] = weekday\nelse:\n    if (row[''PORTROUTE''] == 811):\n        row[''WEEKDAY_END_PV''] = row[''WEEKDAY'']\n    elif (row[''PORTROUTE''] >= 600):\n        row[''WEEKDAY_END_PV''] = 1\n    else:\n        row[''WEEKDAY_END_PV''] = row[''WEEKDAY'']');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 2, 'am_pm_night_pv', 'am_pm_night_pv               ',
        'if row[''PORTROUTE''] == 811 and row[''AM_PM_NIGHT''] == 2:\n    row[''AM_PM_NIGHT_PV''] = 1\nelif row[''PORTROUTE''] == 811 or row[''PORTROUTE''] == 812:\n    row[''AM_PM_NIGHT_PV''] = row[''AM_PM_NIGHT'']\nelse:\n    row[''AM_PM_NIGHT_PV''] = 1');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 3, 'mig_flag_pv', 'mig_flag_pv               ',
        'if row[''LOSKEY''] > 0:\n    row[''MIG_FLAG_PV''] = 1\nelse:\n    row[''MIG_FLAG_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 4, 'shift_flag_pv', 'shift_flag_pv               ',
        'if row[''PORTROUTE''] < 600 or row[''PORTROUTE''] > 900:\n    row[''SHIFT_FLAG_PV''] = 1\nelse:\n    row[''SHIFT_FLAG_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 5, 'crossings_flag_pv', 'crossings_flag_pv               ',
        'if row[''PORTROUTE''] < 600 or row[''PORTROUTE''] > 900:\n    row[''CROSSINGS_FLAG_PV''] = 0\nelse:\n    row[''CROSSINGS_FLAG_PV''] = 1');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 6, 'shift_port_grp_pv', 'shift_port_grp_pv               ',
        'if row[''PORTROUTE''] >= 161 and row[''PORTROUTE''] <= 165:\n    row[''SHIFT_PORT_GRP_PV''] = ''LHR Transits''\nelif row[''PORTROUTE''] >= 171 and row[''PORTROUTE''] <= 175:\n    row[''SHIFT_PORT_GRP_PV''] = ''LHR Mig Transits''\nelse:\n    #  row[''SHIFT_PORT_GRP_PV''] = str(row[''PORTROUTE'']).rjust(3,'' '')\n    row[''SHIFT_PORT_GRP_PV''] = str(int(row[''PORTROUTE'']))');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 7, 'nr_flag_pv', 'nr_flag_pv               ',
        'if row[''RESPNSE''] > 0 and row[''RESPNSE''] < 4:\n    row[''NR_FLAG_PV''] = 0\nelif row[''RESPNSE''] >= 4 and row[''RESPNSE''] < 7:\n    row[''NR_FLAG_PV''] = 1\nelse:\n    row[''NR_FLAG_PV''] = 2');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 8, 'nr_port_grp_pv', 'nr_port_grp_pv               ',
        'row[''NR_PORT_GRP_PV''] = row[''PORTROUTE'']');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 9, 'mins_flag_pv', 'mins_flag_pv               ',
        'if row[''TYPEINTERVIEW''] == 1:\n    row[''MINS_FLAG_PV''] = 2\nelif row[''RESPNSE''] == 1 or row[''RESPNSE''] == 2:\n    row[''MINS_FLAG_PV''] = 0\nelif row[''RESPNSE''] == 3:\n    row[''MINS_FLAG_PV''] = 1\nelse:\n    row[''MINS_FLAG_PV''] = 3');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 10, 'imbal_eligible_pv', 'imbal_eligible_pv               ',
        'if not math.isnan(row[''FLOW'']) and (row[''RESPNSE''] > 0) and (row[''RESPNSE''] < 3) and ((row[''PURPOSE''] != 23) and (row[''PURPOSE''] != 24) and (row[''PURPOSE''] < 71 or math.isnan(row[''PURPOSE'']))) and (math.isnan(row[''INTENDLOS'']) or (row[''INTENDLOS''] < 2) or (row[''INTENDLOS''] > 7)):\n    row[''IMBAL_ELIGIBLE_PV''] = 1\nelse:\n    row[''IMBAL_ELIGIBLE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 11, 'imbal_port_grp_pv', 'imbal_port_grp_pv               ',
        'if row[''PORTROUTE''] in (111,113,119,161,171):\n    row[''IMBAL_PORT_GRP_PV''] = 1 \nelif row[''PORTROUTE''] in (121,123,129,151,153,162,165,172,175):\n    row[''IMBAL_PORT_GRP_PV''] = 2 \nelif row[''PORTROUTE''] in (131,132,133,134,135,163,173):\n    row[''IMBAL_PORT_GRP_PV''] = 3 \nelif row[''PORTROUTE''] in (141,142,143,144,145,164,174):\n    row[''IMBAL_PORT_GRP_PV''] = 4 \nelif row[''PORTROUTE''] in (191,192,193):\n    row[''IMBAL_PORT_GRP_PV''] = 5 \nelif row[''PORTROUTE''] in (181,183,189,199):\n    row[''IMBAL_PORT_GRP_PV''] = 6 \nelif row[''PORTROUTE''] in (211,221,213,223,231,233,234,219):\n    row[''IMBAL_PORT_GRP_PV''] = 7 \nelif row[''PORTROUTE''] in (201,202,203):\n    row[''IMBAL_PORT_GRP_PV''] = 8 \nelif row[''PORTROUTE''] in (311,321,313,461,351,371,381,382,391,393,401,411,241,243,441,443,451,361,481,421,319,471):\n    row[''IMBAL_PORT_GRP_PV''] = 9 \nelif row[''PORTROUTE''] in (671,672,681,682,691,692,731,641):\n    row[''IMBAL_PORT_GRP_PV''] = 10 \nelif row[''PORTROUTE''] in (611,612,701,711,721,722):\n    row[''IMBAL_PORT_GRP_PV''] = 11 \nelif row[''PORTROUTE''] in (621,631,632,633,634,635,636,651,661,662):\n    row[''IMBAL_PORT_GRP_PV''] = 12 \nelif row[''PORTROUTE''] in (911,913):\n    row[''IMBAL_PORT_GRP_PV''] = 13 \nelif row[''PORTROUTE''] == 921:\n    row[''IMBAL_PORT_GRP_PV''] = 14 \nelif row[''PORTROUTE''] == 811:\n    row[''IMBAL_PORT_GRP_PV''] = 15 \nelif row[''IMBAL_PORT_GRP_PV''] == 9999:\n    row[''IMBAL_PORT_GRP_PV''] = 16 \nelif row[''PORTROUTE''] == 951:\n    row[''IMBAL_PORT_GRP_PV''] = 17 \nelif row[''PORTROUTE''] == 812:\n    row[''IMBAL_PORT_GRP_PV''] = 18');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 12, 'imbal_port_fact_pv', 'imbal_port_fact_pv               ',
        'if row[''IMBAL_PORT_GRP_PV''] == 1 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 1.01\nelif row[''IMBAL_PORT_GRP_PV''] == 1 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 0.99\nelif row[''IMBAL_PORT_GRP_PV''] == 2 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.98\nelif row[''IMBAL_PORT_GRP_PV''] == 2 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.00\nelif row[''IMBAL_PORT_GRP_PV''] == 3 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 1.00\nelif row[''IMBAL_PORT_GRP_PV''] == 3 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.00\nelif row[''IMBAL_PORT_GRP_PV''] == 4 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 1.00\nelif row[''IMBAL_PORT_GRP_PV''] == 4 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.00\nelif row[''IMBAL_PORT_GRP_PV''] == 5 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.96\nelif row[''IMBAL_PORT_GRP_PV''] == 5 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.04\nelif row[''IMBAL_PORT_GRP_PV''] == 6 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.89\nelif row[''IMBAL_PORT_GRP_PV''] == 6 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.08\nelif row[''IMBAL_PORT_GRP_PV''] == 7 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.98\nelif row[''IMBAL_PORT_GRP_PV''] == 7 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.03\nelif row[''IMBAL_PORT_GRP_PV''] == 8 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.96\nelif row[''IMBAL_PORT_GRP_PV''] == 8 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.04\nelif row[''IMBAL_PORT_GRP_PV''] == 9 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.86\nelif row[''IMBAL_PORT_GRP_PV''] == 9 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.09\nelif row[''IMBAL_PORT_GRP_PV''] == 10 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.98\nelif row[''IMBAL_PORT_GRP_PV''] == 10 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.00\nelif row[''IMBAL_PORT_GRP_PV''] == 11 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.85\nelif row[''IMBAL_PORT_GRP_PV''] == 11 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.16\nelif row[''IMBAL_PORT_GRP_PV''] == 12 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.94\nelif row[''IMBAL_PORT_GRP_PV''] == 12 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.06\nelif row[''IMBAL_PORT_GRP_PV''] == 13 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.99\nelif row[''IMBAL_PORT_GRP_PV''] == 13 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.01\nelif row[''IMBAL_PORT_GRP_PV''] == 14 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.99\nelif row[''IMBAL_PORT_GRP_PV''] == 14 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.01\nelif row[''IMBAL_PORT_GRP_PV''] == 15 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.96\nelif row[''IMBAL_PORT_GRP_PV''] == 15 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.05\nelif row[''IMBAL_PORT_GRP_PV''] == 16 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.96\nelif row[''IMBAL_PORT_GRP_PV''] == 16 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.05\nelif row[''IMBAL_PORT_GRP_PV''] == 17 and row[''ARRIVEDEPART''] == 1:\n    row[''IMBAL_PORT_FACT_PV''] = 0.99\nelif row[''IMBAL_PORT_GRP_PV''] == 17 and row[''ARRIVEDEPART''] == 2:\n    row[''IMBAL_PORT_FACT_PV''] = 1.01\nelif row[''IMBAL_PORT_GRP_PV''] == 18:\n    row[''IMBAL_PORT_FACT_PV''] = 1.00\nelse:\n    row[''IMBAL_PORT_FACT_PV''] = 1.0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 13, 'stay_imp_flag_pv', 'stay_imp_flag_pv               ',
        'if math.isnan(row[''NUMNIGHTS'']) or row[''NUMNIGHTS''] == 999:\n    row[''STAY_IMP_FLAG_PV''] = 1\nelse:\n    row[''STAY_IMP_FLAG_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 14, 'stay_imp_eligible_pv', 'stay_imp_eligible_pv               ',
        'if row[''FLOW''] in (1,4,5,8) and row[''MINS_FLAG_PV''] == 0 and row[''PURPOSE''] != 80:\n    row[''STAY_IMP_ELIGIBLE_PV''] = 1\nelse:\n    row[''STAY_IMP_ELIGIBLE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 15, 'StayImpCtryLevel1_pv', 'StayImpCtryLevel1_pv               ',
        'if row[''UKFOREIGN''] == 1:\n    if not math.isnan(row[''COUNTRYVISIT'']):\n        row[''STAYIMPCTRYLEVEL1_PV''] = int(row[''COUNTRYVISIT''])\nif row[''UKFOREIGN''] == 2:\n    if not math.isnan(row[''RESIDENCE'']):\n        row[''STAYIMPCTRYLEVEL1_PV''] = int(row[''RESIDENCE''])');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 16, 'StayImpCtryLevel2_pv', 'StayImpCtryLevel2_pv               ',
        'if row[''STAYIMPCTRYLEVEL1_PV''] in (830,831,832,833,931,372):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 1\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (250,56,442,528,492):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 2\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (620,621,911,912,20,292):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 3\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (276,40,756,438,208):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 4\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (470,901,902,380,792,300,674):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 5\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (352,248,246,578,744,752,234):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 6\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (70,191,807,499,951,688,705,100,642,203,703):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 7\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (348,616,8,643,51,31,112,233,268,398,417,428,440,498,762,795,804,860):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 8\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (12,434,504,729,728,788,818,732):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 11\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (270,288,566,694,654,404,426,454,480,690,834,800,894,72,716,204,266,324,624,384,430,466,478,562,686,768,854,24,108,120,140,148,178,180,231,450,508,646,706,262,10,132,174,226,260,175,638,678,232):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 12\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (748,710,516):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 13\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (400,376,275,422,887):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 14\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (48,414,512,634,784,364,368,682,760):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 15\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (462,50,144,356,586):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 21\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (344,156,496,524):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 22\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (96,458,360,608):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 23\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (702,392,158,764):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 24\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (4,104,116,410,418,446,704,626,408):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 25\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (334,36,554):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 31\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (242,598,258,296,316,581,580,584,583,16,772,540,74,162,166,184,520,570,574,585,612,882,90,776,798,548,876):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 32\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (124,666,304):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 41\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (840,630,850):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 42\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (60,388,780,28,44,52,92,136,212,308,500,660,659,662,670,796,192,214,312,652,663,332,474,531,533,534,535,84,328):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 43\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (484,340,320,222,558,188,862,591):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 44\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (76,68,170,218,604,862,254,740):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 45\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (238,32,858,600,152):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 46\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (40,44):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 51\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (41,45,46,47,49):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 52\nelif row[''STAYIMPCTRYLEVEL1_PV''] in (0,958,969,981):\n    row[''STAYIMPCTRYLEVEL2_PV''] = 91\nelse:\n    row[''STAYIMPCTRYLEVEL2_PV''] = 99');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 17, 'StayImpCtryLevel3_pv', 'StayImpCtryLevel3_pv               ',
        'if row[''STAYIMPCTRYLEVEL2_PV''] in (1,2,4,6,8):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 1\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (3,5,7):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 2\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (11,12,13):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 3\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (14,15):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 4\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (21,22,23,24,25):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 5\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (31,32):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 6\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (41,42):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 7\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (43,44,45,46):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 8\nelif row[''STAYIMPCTRYLEVEL2_PV''] in (51,52):\n    row[''STAYIMPCTRYLEVEL3_PV''] = 9\nelif row[''STAYIMPCTRYLEVEL2_PV''] == 91:\n    row[''STAYIMPCTRYLEVEL3_PV''] = 10\nelse:\n    row[''STAYIMPCTRYLEVEL3_PV''] = 99');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 18, 'StayImpCtryLevel4_pv', 'StayImpCtryLevel4_pv               ',
        'if row[''STAYIMPCTRYLEVEL2_PV''] >= 1 and row[''STAYIMPCTRYLEVEL2_PV''] <= 8:\n    row[''STAYIMPCTRYLEVEL4_PV''] = 1\nelif row[''STAYIMPCTRYLEVEL2_PV''] >= 11 and row[''STAYIMPCTRYLEVEL2_PV''] <= 15:\n    row[''STAYIMPCTRYLEVEL4_PV''] = 2\nelif row[''STAYIMPCTRYLEVEL2_PV''] >= 21 and row[''STAYIMPCTRYLEVEL2_PV''] <= 32:\n    row[''STAYIMPCTRYLEVEL4_PV''] = 3\nelif row[''STAYIMPCTRYLEVEL2_PV''] >= 41 and row[''STAYIMPCTRYLEVEL2_PV''] <= 46:\n    row[''STAYIMPCTRYLEVEL4_PV''] = 4\nelse:\n    row[''STAYIMPCTRYLEVEL4_PV''] = 5');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 19, 'stay_purpose_grp_pv', 'stay_purpose_grp_pv               ',
        'if row[''PURPOSE''] in (20,21,22,24,25):\n    row[''STAY_PURPOSE_GRP_PV''] = 1 \nelif row[''PURPOSE''] in (10,15,16):\n    row[''STAY_PURPOSE_GRP_PV''] = 2 \nelif row[''PURPOSE''] in (47,60,61,62,63,64,65,66):\n    row[''STAY_PURPOSE_GRP_PV''] = 3 \nelif row[''PURPOSE''] in (11,12):\n    row[''STAY_PURPOSE_GRP_PV''] = 4 \nelif row[''PURPOSE''] in (17,18,70,71):\n    row[''STAY_PURPOSE_GRP_PV''] = 5 \nelse:\n    row[''STAY_PURPOSE_GRP_PV''] = 6');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 20, 'fares_imp_flag_pv', 'fares_imp_flag_pv               ',
        'if (row[''DVFARE''] == 999999) or math.isnan(row[''DVFARE'']) or (row[''DVFARE''] == 0):\n    row[''FARES_IMP_FLAG_PV''] = 1\nelse:\n    row[''FARES_IMP_FLAG_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 21, 'fares_imp_eligible_pv', 'fares_imp_eligible_pv               ',
        'if (((row[''FAREKEY''] == ''1'') or (row[''FAREKEY''] == ''1.0'')) or ((row[''FARES_IMP_FLAG_PV'']) == 1)) and ((row[''MINS_FLAG_PV'']) == 0):\n    row[''FARES_IMP_ELIGIBLE_PV''] = 1\nelse:\n    row[''FARES_IMP_ELIGIBLE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 22, 'discnt_f1_pv', 'discnt_f1_pv               ',
        'if row[''FLOW''] in (1, 2, 3, 4, 5, 6, 7, 8):\n    row[''DISCNT_F1_PV''] = 0.85');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 23, 'discnt_package_cost_pv', 'discnt_package_cost_pv               ',
        'if row[''PACKAGE''] in (1 ,2):\n    if row[''DVPACKCOST''] != 999999:\n        if not row[''DVPACKCOST''] == None:\n            row[''DISCNT_PACKAGE_COST_PV''] = row[''DVPACKCOST''] * 0.85\nelse:\n    row[''DISCNT_PACKAGE_COST_PV''] = row[''DVPACKCOST'']\nrow[''DISCNT_PACKAGE_COST_PV''] = round(row[''DISCNT_PACKAGE_COST_PV''], 2)');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 24, 'discnt_f2_pv', 'discnt_f2_pv               ',
        'if row[''PACKAGE''] in (1,2) and row[''FLOW''] in (1,2,3,4,5,6,7,8):\n    row[''DISCNT_F2_PV''] = 0.85');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 25, 'fage_pv', 'fage_pv               ',
        'if row[''KIDAGE''] in (0, 1):\n    row[''FAGE_PV''] = 1\nelif (row[''KIDAGE''] >= 2) and (row[''KIDAGE''] <= 15):\n    row[''FAGE_PV''] = 2\nelse:\n    row[''FAGE_PV''] = 6\n\nif (row[''AGE''] > 1) or math.isnan(row[''AGE'']):\n    row[''FAGE_PV''] = 6\nelif (row[''AGE''] < 2) and math.isnan(row[''KIDAGE'']):\n    row[''FAGE_PV''] = 2');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 26, 'type_pv', 'type_pv               ',
        'if row[''PURPOSE''] in (20,21,22):\n    row[''TYPE_PV''] = 1\nelse:\n    row[''TYPE_PV''] = 2');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 28, 'ukport1_pv', 'ukport1_pv               ',
        'if (row[''PORTROUTE''] >= 111 and row[''PORTROUTE''] <= 119) or row[''PORTROUTE''] in (161,171):\n    row[''UKPORT1_PV''] = 110\nelif (row[''PORTROUTE''] >= 121 and row[''PORTROUTE''] <= 129) or row[''PORTROUTE''] in (162,172):\n    row[''UKPORT1_PV''] = 120\nelif (row[''PORTROUTE''] >= 131 and row[''PORTROUTE''] <= 139) or row[''PORTROUTE''] in (163,173):\n    row[''UKPORT1_PV''] = 130\nelif (row[''PORTROUTE''] >= 141 and row[''PORTROUTE''] <= 149) or row[''PORTROUTE''] in (164,174):\n    row[''UKPORT1_PV''] = 140\nelif (row[''PORTROUTE''] >= 151 and row[''PORTROUTE''] <= 159) or row[''PORTROUTE''] in (165,175):\n    row[''UKPORT1_PV''] = 150\nelif row[''PORTROUTE''] >= 181 and row[''PORTROUTE''] <= 189:\n    row[''UKPORT1_PV''] = 180\nelif row[''PORTROUTE''] >= 191 and row[''PORTROUTE''] <= 199:\n    row[''UKPORT1_PV''] = 190\nelif row[''PORTROUTE''] >= 201 and row[''PORTROUTE''] <= 209:\n    row[''UKPORT1_PV''] = 200\nelif row[''PORTROUTE''] >= 211 and row[''PORTROUTE''] <= 219:\n    row[''UKPORT1_PV''] = 210\nelif row[''PORTROUTE''] >= 221 and row[''PORTROUTE''] <= 229:\n    row[''UKPORT1_PV''] = 220\nelif row[''PORTROUTE''] >= 241 and row[''PORTROUTE''] <= 249:\n    row[''UKPORT1_PV''] = 240\nelif row[''PORTROUTE''] >= 311 and row[''PORTROUTE''] <= 319:\n    row[''UKPORT1_PV''] = 310\nelse:\n    row[''UKPORT1_PV''] = row[''PORTROUTE'']');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 29, 'ukport2_pv', 'ukport2_pv               ',
        'if row[''UKPORT1_PV''] >= 110 and row[''UKPORT1_PV''] <= 150:\n    row[''UKPORT2_PV''] = 1\nelif row[''UKPORT1_PV''] in (180,190):\n    row[''UKPORT2_PV''] = 2\nelif (row[''UKPORT1_PV''] >= 210 and row[''UKPORT1_PV''] <= 231):\n    row[''UKPORT2_PV''] = 3\nelif row[''UKPORT1_PV''] == 200:\n    row[''UKPORT2_PV''] = 4\nelif row[''UKPORT1_PV''] == 340:\n    row[''UKPORT2_PV''] = 5\nelif row[''UKPORT1_PV''] in (381,391,451):\n    row[''UKPORT2_PV''] = 10\nelif row[''UKPORT1_PV''] in (401,411,441):\n    row[''UKPORT2_PV''] = 11\nelif row[''UKPORT1_PV''] in (310,371):\n    row[''UKPORT2_PV''] = 12\nelif row[''UKPORT1_PV''] == 421:\n    row[''UKPORT2_PV''] = 13\nelif row[''UKPORT1_PV''] in (351,361):\n    row[''UKPORT2_PV''] = 14\nelif row[''UKPORT1_PV''] in (461,481):\n    row[''UKPORT2_PV''] = 15\nelif row[''UKPORT1_PV''] in (611,612):\n    row[''UKPORT2_PV''] = 21\nelif row[''UKPORT1_PV''] in (631,632,633,634) or (row[''UKPORT1_PV''] >= 651 and row[''UKPORT1_PV''] <= 662):\n    row[''UKPORT2_PV''] = 22\nelif row[''UKPORT1_PV''] in (671, 672):\n    row[''UKPORT2_PV''] = 23\nelif row[''UKPORT1_PV''] >= 681 and row[''UKPORT1_PV''] <= 692:\n    row[''UKPORT2_PV''] = 24\nelif row[''UKPORT1_PV''] in (701,711):\n    row[''UKPORT2_PV''] = 25\nelif row[''UKPORT1_PV''] in (721,722):\n    row[''UKPORT2_PV''] = 26\nelif row[''UKPORT1_PV''] == 641:\n    row[''UKPORT2_PV''] = 27\nelif row[''UKPORT1_PV''] in (811,812):\n    row[''UKPORT2_PV''] = 31\nelif row[''UKPORT1_PV''] >= 911 and row[''UKPORT1_PV''] <= 951:\n    row[''UKPORT2_PV''] = 32\nelse:\n    row[''UKPORT2_PV''] = 99');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 30, 'ukport3_pv', 'ukport3_pv               ',
        'if row[''UKPORT2_PV''] in (1,2,4,13):\n    row[''UKPORT3_PV''] = 1\nelif row[''UKPORT2_PV''] in (3,10,11):\n    row[''UKPORT3_PV''] = 2\nelif row[''UKPORT2_PV''] in (12,14):\n    row[''UKPORT3_PV''] = 3\nelif row[''UKPORT2_PV''] in (21,22):\n    row[''UKPORT3_PV''] = 4\nelif row[''UKPORT2_PV''] in (23,24):\n    row[''UKPORT3_PV''] = 5\nelif row[''UKPORT2_PV''] in (25,26):\n    row[''UKPORT3_PV''] = 6\nelif row[''UKPORT2_PV''] == 27:\n    row[''UKPORT3_PV''] = 7\nelif row[''UKPORT2_PV''] == 31:\n    row[''UKPORT3_PV''] = 8\nelif row[''UKPORT2_PV''] == 32:\n    row[''UKPORT3_PV''] = 9\nelse:\n    row[''UKPORT3_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 31, 'ukport4_pv', 'ukport4_pv               ',
        'if row[''UKPORT3_PV''] in (1,2,3,9):\n    row[''UKPORT4_PV''] = 1\nelif row[''UKPORT3_PV''] in (4,5,6,7,8):\n    row[''UKPORT4_PV''] = 2\nelse:\n    row[''UKPORT4_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 32, 'osport1_pv', 'osport1_pv               ',
        'row[''OSPORT1_PV''] = row[''DVPORTCODE'']\n\nif not math.isnan(row[''CHANGECODE'']):\n    row[''OSPORT1_PV''] = row[''CHANGECODE'']\n\nif row[''OSPORT1_PV''] in (999998,999999):\n    row[''OSPORT1_PV''] = float(''NaN'')');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 33, 'osport2_pv', 'osport2_pv               ',
        'if row[''UKPORT1_PV''] == 641:\n    if not math.isnan(row[''OSPORT1_PV'']):\n        row[''OSPORT2_PV''] = int(float(row[''OSPORT1_PV''])/ 1000.0)\nelse:\n    if not math.isnan(row[''OSPORT1_PV'']):\n        row[''OSPORT2_PV''] = int(float(row[''OSPORT1_PV''])/ 1000.0)\n\nif row[''UKFOREIGN''] == 1 and math.isnan(row[''OSPORT1_PV'']):\n    row[''OSPORT2_PV''] = row[''COUNTRYVISIT'']\n\nif row[''UKFOREIGN''] == 2 and math.isnan(row[''OSPORT1_PV'']):\n    row[''OSPORT2_PV''] = row[''RESIDENCE'']\n\nif row[''OSPORT2_PV''] == 292:\n    row[''OSPORT2_PV''] = 2500 \nelif row[''OSPORT2_PV''] == 292:\n    row[''OSPORT2_PV''] = 912\nelif row[''OSPORT2_PV''] == 470:\n    row[''OSPORT2_PV''] = 300\nelif row[''OSPORT2_PV''] == 831:\n    row[''OSPORT2_PV''] = 372\nelif row[''OSPORT2_PV''] == 832:\n    row[''OSPORT2_PV''] = 372\nelif row[''OSPORT2_PV''] == 833:\n    row[''OSPORT2_PV''] = 372');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 34, 'osport3_pv', 'osport3_pv               ',
        'if row[''OSPORT2_PV''] in (40,56,250,276,372,438,442,492,528,756,830,831,832,833,922,924,926,931):\n    row[''OSPORT3_PV''] = 1\nelif row[''OSPORT2_PV''] in (208,233,234,246,248,352,428,440,578,744,752):\n    row[''OSPORT3_PV''] = 2\nelif row[''OSPORT2_PV''] in (31,51,112,203,268,348,498,616,642,643,703,804):\n    row[''OSPORT3_PV''] = 3\nelif row[''OSPORT2_PV''] in (8,20,70,100,191,292,300,336,380,470,499,620,621,674,688,705,792,807,901,902,911,912,951):\n    row[''OSPORT3_PV''] = 4\nelif row[''OSPORT2_PV''] in (12,434,504,732,788,818):\n    row[''OSPORT3_PV''] = 5\nelif row[''OSPORT2_PV''] in (24,72,108,120,132,140,148,174,175,178,180,204,226,231,232,262,266,270,288,324,384,404,426,430,450,454,466,478,480,508,516,562,566,624,638,646,654,678,686,690,694,706,710,716,748,768,800,834,854,894):\n    row[''OSPORT3_PV''] = 6\nelif row[''OSPORT2_PV''] in (60,124,304,840):\n    row[''OSPORT3_PV''] = 7\nelif row[''OSPORT2_PV''] in (28,32,44,52,68,76,84,92,136,152,170,188,192,212,214,218,222,238,254,308,312,320,328,332,340,388,474,484,500,531,533,534,535,558,591,600,604,630,652,659,660,662,663,666,670,740,780,796,850,858,862):\n    row[''OSPORT3_PV''] = 8\nelif row[''OSPORT2_PV''] in (4,50,64,96,104,116,144,156,158,344,356,360,392,398,408,410,417,418,446,458,462,496,524,586,608,626,702,704,762,764,795,860):\n    row[''OSPORT3_PV''] = 9\nelif row[''OSPORT2_PV''] in (48,275,364,368,376,400,414,422,512,634,682,760,784,887):\n    row[''OSPORT3_PV''] = 10\nelif row[''OSPORT2_PV''] in (10,16,36,74,90,162,166,184,239,242,258,260,296,316,334,520,540,548,554,570,574,580,581,583,84,585,598,612,772,776,798,876,882):\n    row[''OSPORT3_PV''] = 11\nelif row[''OSPORT2_PV''] in (940,941,942,943,944,945,946,947,949):\n    row[''OSPORT3_PV''] = 12\nelse:\n    row[''OSPORT3_PV''] = 13');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 35, 'osport4_pv', 'osport4_pv               ',
        'if row[''OSPORT3_PV''] in (1,2):\n    row[''OSPORT4_PV''] = 1\nelse:\n    row[''OSPORT4_PV''] = 2');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 36, 'apd_pv', 'apd_pv               ',
        'if row[''OSPORT2_PV''] in (2,8,12,40,56,70,100,112,191,203,208,233,234,246,250,251,276,292,300):\n    APDBAND = 1\nelif row[''OSPORT2_PV''] in (301,304,348,352,372,380,428,434,440,442,470,492,498,499,504,528,578,616):\n    APDBAND = 1\nelif row[''OSPORT2_PV''] in (620,621,642,688,703,705,732,752,756,788,792,804,807,831,832,833):\n    APDBAND = 1\nelif row[''OSPORT2_PV''] in (901,902,911,912,913,921,923,924,926,931,933,951,2500,1000,210,2200):\n    APDBAND = 1\nelse:\n    APDBAND = 2\n\nif row[''FLOW''] > 4:\n    row[''APD_PV''] = 0\nelif APDBAND == 1:\n    row[''APD_PV''] = 13\nelse:\n    row[''APD_PV''] = 71');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 37, 'qmfare_pv', 'qmfare_pv               ',
        'if row[''OSPORT3_PV''] == 12 and (row[''MINS_FLAG_PV''] == 0 or math.isnan(row[''MINS_FLAG_PV''])):\n    row[''QMFARE_PV''] = 1500\nelse:\n    row[''QMFARE_PV''] = None');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 38, 'duty_free_pv', 'duty_free_pv               ',
        'if row[''FLOW''] == 1 and ((row[''PURPOSE''] < 80 and row[''PURPOSE''] != 71) or math.isnan(row[''PURPOSE''])):\n    row[''DUTY_FREE_PV''] = 15\nelse:\n    row[''DUTY_FREE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 39, 'spend_imp_eligible_pv', 'spend_imp_eligible_pv               ',
        'if(row[''FLOW''] in (1,4,5,8)and (row[''PURPOSE''] < 80 or math.isnan(row[''PURPOSE'']))and row[''PURPOSE''] != 23 and row[''PURPOSE''] != 24 and row[''MINS_FLAG_PV''] == 0):\n    row[''SPEND_IMP_ELIGIBLE_PV''] = 1\nelse:\n    row[''SPEND_IMP_ELIGIBLE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 40, 'uk_os_pv', 'uk_os_pv               ',
        'if row[''FLOW''] in (1,5):\n    row[''UK_OS_PV''] = 2\n\nif row[''FLOW''] in (4,8):\n    row[''UK_OS_PV''] = 1');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 41, 'pur1_pv', 'pur1_pv               ',
        'if row[''DVPACKAGE''] in (1,2):\n    row[''IND''] = 1\n\nif row[''DVPACKAGE''] == 9 or math.isnan(row[''DVPACKAGE'']):\n    row[''IND''] = 0\n\nif row[''PURPOSE''] in (10,14,17,18):\n    row[''PUR1_PV''] = 2\nelif row[''PURPOSE''] in (20,21,22):\n    row[''PUR1_PV''] = 3\nelif row[''PURPOSE''] in (11,12):\n    row[''PUR1_PV''] = 4\nelif row[''PURPOSE''] in (47,60,61,62,63,64,65,66):\n    row[''PUR1_PV''] = 5\nelif row[''PURPOSE''] == 71:\n    row[''PUR1_PV''] = 6\nelse:\n    row[''PUR1_PV''] = 7\n\nif row[''IND''] == 1 and row[''PUR1_PV''] == 2:\n    row[''PUR1_PV''] = 1');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 42, 'pur2_pv', 'pur2_pv               ',
        'if row[''PURPOSE''] in (10,14,17,18,11,12):\n    row[''PUR2_PV''] = 2\nelif row[''PURPOSE''] in (20,21,22):\n    row[''PUR2_PV''] = 3\nelif row[''PURPOSE''] == 71:\n    row[''PUR2_PV''] = 4\nelif math.isnan(row[''PURPOSE'']):\n    row[''PUR2_PV''] = None\nelse:\n    row[''PUR2_PV''] = 5\n\nif row[''IND''] == 1 and row[''PUR2_PV''] == 2:\n    row[''PUR2_PV''] = 1');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 43, 'pur3_pv', 'pur3_pv               ',
        'if row[''PURPOSE''] in (20,21,22):\n    row[''PUR3_PV''] = 1\nelif math.isnan(row[''PURPOSE'']):\n    row[''PUR3_PV''] = None\nelse:\n    row[''PUR3_PV''] = 2');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 44, 'dur1_pv', 'dur1_pv               ',
        'if row[''STAY''] == 0:\n    row[''DUR1_PV''] = 0\nelif row[''STAY''] >= 1 and row[''STAY''] <= 7:\n    row[''DUR1_PV''] = 1\nelif row[''STAY''] >= 8 and row[''STAY''] <= 21:\n    row[''DUR1_PV''] = 2\nelif row[''STAY''] >= 22 and row[''STAY''] <= 35:\n    row[''DUR1_PV''] = 3\nelif row[''STAY''] >= 36 and row[''STAY''] <= 91:\n    row[''DUR1_PV''] = 4\nelif row[''STAY''] >= 92:\n    row[''DUR1_PV''] = 5');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 45, 'dur2_pv', 'dur2_pv               ',
        'if row[''STAY''] >= 0 and row[''STAY''] <= 30:\n    row[''DUR2_PV''] = 1\nelif row[''STAY''] >= 31:\n    row[''DUR2_PV''] = 2');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 46, 'imbal_ctry_fact_pv', 'imbal_ctry_fact_pv               ',
        'if row[''RESIDENCE''] == 352 or row[''RESIDENCE''] == 40 or row[''RESIDENCE''] in (292, 470, 902, 901) or row[''RESIDENCE''] == 792 or row[''RESIDENCE''] == 620 or row[''RESIDENCE''] == 621 or row[''RESIDENCE''] in (973, 70, 191, 807, 499, 688, 951, 705)  or row[''RESIDENCE''] == 234:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.02\nelif row[''RESIDENCE''] == 56 or row[''RESIDENCE''] == 442:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.04\nelif (row[''RESIDENCE''] == 250) or row[''RESIDENCE''] == 492:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.0\nelif row[''RESIDENCE''] == 276:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.06\nelif (row[''RESIDENCE''] == 380) or row[''RESIDENCE''] == 674:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.1\nelif row[''RESIDENCE''] == 528:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.00\nelif row[''RESIDENCE''] == 208:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.08\nelif row[''RESIDENCE''] in (246, 248, 578, 744, 752):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.02\nelif (row[''RESIDENCE''] == 300):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.01\nelif (row[''RESIDENCE''] == 911) or row[''RESIDENCE''] == 20 or row[''RESIDENCE''] == 732:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.01\nelif row[''RESIDENCE''] == 756 or row[''RESIDENCE''] == 438:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.04\nelif row[''RESIDENCE''] in (100, 642, 203, 703, 348, 616, 8, 643, 51, 31, 112, 233, 268, 398, 417, 428, 440, 498, 762, 795, 804, 860):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.12\nelif row[''RESIDENCE''] in (12, 434, 504, 736, 788, 818) or row[''RESIDENCE''] in (48, 400, 414, 512, 634, 784, 275, 376, 364, 368, 422, 682, 887, 760):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.0\nelif row[''RESIDENCE''] in (270, 288, 566, 694, 654, 404, 426, 454, 480, 690, 834, 800, 894, 72, 748) or row[''RESIDENCE''] in (204, 266, 324, 624, 384, 430, 466, 478, 562, 686, 768, 854, 24, 108, 120, 140, 148, 178, 180, 231, 450, 450, 508, 646, 706, 262, 10, 132, 174, 175, 226, 226, 260, 638, 678, 232) or row[''RESIDENCE''] in (156, 408, 496):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.0\nelif row[''RESIDENCE''] in (710, 516):\n    row[''IMBAL_CTRY_FACT_PV''] = 0.96\nelif row[''RESIDENCE''] in (36, 334, 554):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.0\nelif row[''RESIDENCE''] in (242, 598, 598, 258, 16, 296, 316, 580, 581, 583, 584, 772, 540, 74, 86, 90, 162, 166, 184, 296, 520, 548, 570, 574, 585, 612, 776, 798, 876, 882) or row[''RESIDENCE''] in (50, 96, 144, 458, 702) or (row[''RESIDENCE''] == 356) or row[''RESIDENCE''] in (586, 4, 64, 104, 116, 360, 410, 418, 446, 524, 608, 58, 764, 704, 626):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.12\nelif row[''RESIDENCE''] == 344:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.02\nelif (row[''RESIDENCE''] == 392):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.08\nelif row[''RESIDENCE''] in (60, 388, 780, 28, 44, 52, 92, 136, 212, 308, 500, 659, 660, 662, 670, 796) or row[''RESIDENCE''] in (84, 328, 238, 239):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.02\nelif row[''RESIDENCE''] in (192, 214, 312, 652, 663, 332, 474, 530, 533) or row[''RESIDENCE''] in (32, 76, 484, 68, 152, 170, 218, 600, 604, 858, 862, 188, 222, 320, 340, 558, 254, 591, 740):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.0\nelif row[''RESIDENCE''] == 124 or row[''RESIDENCE''] in (666, 304):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.04\nelif row[''RESIDENCE''] in (840, 630, 850):\n    row[''IMBAL_CTRY_FACT_PV''] = 1.04\nelse:\n    row[''IMBAL_CTRY_FACT_PV''] = 1.0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 47, 'rail_cntry_grp_pv', 'rail_cntry_grp_pv               ',
        'railcountry = 0\n\nif row[''FLOW''] == 5:\n    railcountry = row[''RESIDENCE'']\nelif row[''FLOW''] == 8:\n    railcountry = row[''COUNTRYVISIT'']\n\nif (railcountry == 250):\n    row[''RAIL_CNTRY_GRP_PV''] = 1\nelif railcountry in (208,578,752):\n    row[''RAIL_CNTRY_GRP_PV''] = 2\nelif railcountry == 56:\n    row[''RAIL_CNTRY_GRP_PV''] = 3\nelif railcountry == 276:\n    row[''RAIL_CNTRY_GRP_PV''] = 4\nelif railcountry == 380:\n    row[''RAIL_CNTRY_GRP_PV''] = 5\nelif railcountry in (911,912):\n    row[''RAIL_CNTRY_GRP_PV''] = 6\nelif railcountry == 756:\n    row[''RAIL_CNTRY_GRP_PV''] = 7\nelif railcountry in (40,442,528,620):\n    row[''RAIL_CNTRY_GRP_PV''] = 8\nelif railcountry == 372:\n    row[''RAIL_CNTRY_GRP_PV''] = 9\nelif railcountry == 840:\n    row[''RAIL_CNTRY_GRP_PV''] = 10\nelif railcountry == 124:\n    row[''RAIL_CNTRY_GRP_PV''] = 11\nelif railcountry in (112,100,191,203,246,300,348,973,428,440,807,504,616,642,643,703,705,792,804,233):\n    row[''RAIL_CNTRY_GRP_PV''] = 12');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 48, 'rail_exercise_pv', 'rail_exercise_pv               ',
        'if row[''FLOW''] == 8:\n    if row[''RAIL_CNTRY_GRP_PV''] == 1:\n        row[''RAIL_EXERCISE_PV''] = 11\n    elif row[''RAIL_CNTRY_GRP_PV''] == 2:\n        row[''RAIL_EXERCISE_PV''] = 28\n    elif row[''RAIL_CNTRY_GRP_PV''] == 3:\n        row[''RAIL_EXERCISE_PV''] = 4\n    elif row[''RAIL_CNTRY_GRP_PV''] == 4:\n        row[''RAIL_EXERCISE_PV''] = 19\n    elif row[''RAIL_CNTRY_GRP_PV''] == 5:\n        row[''RAIL_EXERCISE_PV''] = 21\n    elif row[''RAIL_CNTRY_GRP_PV''] == 6:\n        row[''RAIL_EXERCISE_PV''] = 2\n    elif row[''RAIL_CNTRY_GRP_PV''] == 7:\n        row[''RAIL_EXERCISE_PV''] = 12\n    elif row[''RAIL_CNTRY_GRP_PV''] == 8:\n        row[''RAIL_EXERCISE_PV''] = 15\n    elif row[''RAIL_CNTRY_GRP_PV''] == 9:\n        row[''RAIL_EXERCISE_PV''] = 8\n    elif row[''RAIL_CNTRY_GRP_PV''] == 10:\n        row[''RAIL_EXERCISE_PV''] = 0\n    elif row[''RAIL_CNTRY_GRP_PV''] == 11:\n        row[''RAIL_EXERCISE_PV''] = 0\n    elif row[''RAIL_CNTRY_GRP_PV''] == 12:\n        row[''RAIL_EXERCISE_PV''] = 11\nelif row[''FLOW''] == 5:\n    if row[''RAIL_CNTRY_GRP_PV''] == 1:\n        row[''RAIL_EXERCISE_PV''] = 36\n    elif row[''RAIL_CNTRY_GRP_PV''] == 2:\n        row[''RAIL_EXERCISE_PV''] = 44\n    elif row[''RAIL_CNTRY_GRP_PV''] == 3:\n        row[''RAIL_EXERCISE_PV''] = 0\n    elif row[''RAIL_CNTRY_GRP_PV''] == 4:\n        row[''RAIL_EXERCISE_PV''] = 47\n    elif row[''RAIL_CNTRY_GRP_PV''] == 5:\n        row[''RAIL_EXERCISE_PV''] = 21\n    elif row[''RAIL_CNTRY_GRP_PV''] == 6:\n        row[''RAIL_EXERCISE_PV''] = 0\n    elif row[''RAIL_CNTRY_GRP_PV''] == 7:\n        row[''RAIL_EXERCISE_PV''] = 8\n    elif row[''RAIL_CNTRY_GRP_PV''] == 8:\n        row[''RAIL_EXERCISE_PV''] = 1\n    elif row[''RAIL_CNTRY_GRP_PV''] == 9:\n        row[''RAIL_EXERCISE_PV''] = 42\n    elif row[''RAIL_CNTRY_GRP_PV''] == 10:\n        row[''RAIL_EXERCISE_PV''] = 226\n    elif row[''RAIL_CNTRY_GRP_PV''] == 11:\n        row[''RAIL_EXERCISE_PV''] = 37\n    elif row[''RAIL_CNTRY_GRP_PV''] == 12:\n        row[''RAIL_EXERCISE_PV''] = 21\n\nrow[''RAIL_EXERCISE_PV''] = row[''RAIL_EXERCISE_PV''] * 1000');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 49, 'rail_imp_eligible_pv', 'rail_imp_eligible_pv               ',
        'if row[''FLOW''] in (5,8):\n    row[''RAIL_IMP_ELIGIBLE_PV''] = 1\nelse:\n    row[''RAIL_IMP_ELIGIBLE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 50, 'spend_imp_flag_pv', 'spend_imp_flag_pv               ',
        'if math.isnan(row[''SPEND'']):\n    row[''SPEND_IMP_FLAG_PV''] = 1\nelse:\n    row[''SPEND_IMP_FLAG_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 51, 'purpose_pv', 'purpose_pv               ',
        'if row[''PURPOSE''] == 20 or row[''PURPOSE''] == 21 or row[''PURPOSE''] == 22:\n    row[''PURPOSE_PV''] = 1\nelif row[''PURPOSE''] == 10 or row[''PURPOSE''] == 14 or row[''PURPOSE''] == 17 or row[''PURPOSE''] == 18:\n    row[''PURPOSE_PV''] = 2\nelif row[''PURPOSE''] in (47, 60,61,62,63,64,65,66):\n    row[''PURPOSE_PV''] = 3\nelif row[''PURPOSE''] == 11 or row[''PURPOSE''] == 12 or row[''PURPOSE''] == 6:\n    row[''PURPOSE_PV''] = 4\nelse:\n    row[''PURPOSE_PV''] = 5');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 52, 'town_imp_eligible_pv', 'town_imp_eligible_pv               ',
        'if row[''FLOW''] in (1,5)and row[''RESPNSE''] != 5 and (row[''PURPOSE''] <= 89 or row[''PURPOSE''] == 92 or math.isnan(row[''PURPOSE''])):\n    row[''TOWN_IMP_ELIGIBLE_PV''] = 1\nelse:\n    row[''TOWN_IMP_ELIGIBLE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 53, 'reg_imp_eligible_pv', 'reg_imp_eligible_pv               ',
        'if row[''FLOW''] in (1,5)and row[''RESPNSE''] != 5 and (row[''PURPOSE''] <= 89 or row[''PURPOSE''] == 92 or math.isnan(row[''PURPOSE''])):\n    row[''REG_IMP_ELIGIBLE_PV''] = 1\nelse:\n    row[''REG_IMP_ELIGIBLE_PV''] = 0');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 54, 'mins_ctry_grp_pv', 'mins_ctry_grp_pv               ',
        'row[''MINS_CTRY_GRP_PV''] = row[''FLOW'']');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 55, 'mins_port_grp_pv', 'mins_port_grp_pv               ',
        'row[''MINS_PORT_GRP_PV''] = int(row[''PORTROUTE''])');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 56, 'SAMP_PORT_GRP_PV', 'SAMP_PORT_GRP_PV               ',
        'if row[''PORTROUTE''] in (111,113,119,161,171):\n    row[''SAMP_PORT_GRP_PV''] = ''A111''\nelif row[''PORTROUTE''] in (121,123,129,162,172):\n    row[''SAMP_PORT_GRP_PV''] = ''A121''\nelif row[''PORTROUTE''] in (131,132,133,134,135,163,173):\n    row[''SAMP_PORT_GRP_PV''] = ''A131''\nelif row[''PORTROUTE''] in (141,142,143,144,145,164,174):\n    row[''SAMP_PORT_GRP_PV''] = ''A141''\nelif row[''PORTROUTE''] in (151,152,153,154,165,175):\n    row[''SAMP_PORT_GRP_PV''] = ''A151''\nelif row[''PORTROUTE''] in (181,183,189):\n    row[''SAMP_PORT_GRP_PV''] = ''A181''\nelif row[''PORTROUTE''] in (191,192,193,199):\n    row[''SAMP_PORT_GRP_PV''] = ''A191''\nelif row[''PORTROUTE''] in (201,202,203,204):\n    row[''SAMP_PORT_GRP_PV''] = ''A201''\nelif row[''PORTROUTE''] in (211,213,219):\n    row[''SAMP_PORT_GRP_PV''] = ''A211''\nelif row[''PORTROUTE''] in (221,223):\n    row[''SAMP_PORT_GRP_PV''] = ''A221''\nelif row[''PORTROUTE''] in (231,232,233,234):\n    row[''SAMP_PORT_GRP_PV''] = ''A231''\nelif row[''PORTROUTE''] in (241,243,249):\n    row[''SAMP_PORT_GRP_PV''] = ''A241''\nelif row[''PORTROUTE''] in (311,321,313,319):\n    row[''SAMP_PORT_GRP_PV''] = ''A311''\nelif row[''PORTROUTE''] == 331:\n    row[''SAMP_PORT_GRP_PV''] = ''A331''\nelif row[''PORTROUTE''] == 351:\n    row[''SAMP_PORT_GRP_PV''] = ''A351''\nelif row[''PORTROUTE''] == 361:\n    row[''SAMP_PORT_GRP_PV''] = ''A361''\nelif row[''PORTROUTE''] == 371:\n    row[''SAMP_PORT_GRP_PV''] = ''A371''\nelif row[''PORTROUTE''] in (381,382):\n    row[''SAMP_PORT_GRP_PV''] = ''A381''\nelif row[''PORTROUTE''] in (341,391,393):\n    row[''SAMP_PORT_GRP_PV''] = ''A391''\nelif row[''PORTROUTE''] == 401:\n    row[''SAMP_PORT_GRP_PV''] = ''A401''\nelif row[''PORTROUTE''] == 411:\n    row[''SAMP_PORT_GRP_PV''] = ''A411''\nelif row[''PORTROUTE''] in (421,423):\n    row[''SAMP_PORT_GRP_PV''] = ''A421''\nelif row[''PORTROUTE''] in (441,443):\n    row[''SAMP_PORT_GRP_PV''] = ''A441''\nelif row[''PORTROUTE''] == 451:\n    row[''SAMP_PORT_GRP_PV''] = ''A451''\nelif row[''PORTROUTE''] == 461:\n    row[''SAMP_PORT_GRP_PV''] = ''A461''\nelif row[''PORTROUTE''] == 471:\n    row[''SAMP_PORT_GRP_PV''] = ''A471''\nelif row[''PORTROUTE''] == 481:\n    row[''SAMP_PORT_GRP_PV''] = ''A481''\nelif row[''PORTROUTE''] in (611,612,613):\n    row[''SAMP_PORT_GRP_PV''] = ''DCF''\nelif row[''PORTROUTE''] in (621,631,632,633,634,651,652,662):\n    row[''SAMP_PORT_GRP_PV''] = ''SCF''\nelif row[''PORTROUTE''] == 641:\n    row[''SAMP_PORT_GRP_PV''] = ''LHS''\nelif row[''PORTROUTE''] in (635,636,661):\n    row[''SAMP_PORT_GRP_PV''] = ''SLR''\nelif row[''PORTROUTE''] == 671:\n    row[''SAMP_PORT_GRP_PV''] = ''HBN''\nelif row[''PORTROUTE''] == 672:\n    row[''SAMP_PORT_GRP_PV''] = ''HGS''\nelif row[''PORTROUTE''] == 681:\n    row[''SAMP_PORT_GRP_PV''] = ''EGS''\nelif row[''PORTROUTE''] in (701,711,741):\n    row[''SAMP_PORT_GRP_PV''] = ''SSE''\nelif row[''PORTROUTE''] in (721,722):\n    row[''SAMP_PORT_GRP_PV''] = ''SNE''\nelif row[''PORTROUTE''] in (731,682,691,692):\n    row[''SAMP_PORT_GRP_PV''] = ''RSS''\nelif row[''PORTROUTE''] in (811,813):\n    row[''SAMP_PORT_GRP_PV''] = ''T811''\nelif row[''PORTROUTE''] == 812:\n    row[''SAMP_PORT_GRP_PV''] = ''T811''\nelif row[''PORTROUTE''] in (911,913):\n    row[''SAMP_PORT_GRP_PV''] = ''E911''\nelif row[''PORTROUTE''] == 921:\n    row[''SAMP_PORT_GRP_PV''] = ''E921''\nelif row[''PORTROUTE''] == 951:\n    row[''SAMP_PORT_GRP_PV''] = ''E951''\nIrish = 0\nIoM = 0\nChannelI = 0\ndvpc = 0\nif dataset == ''survey'':\n    if not math.isnan(row[''DVPORTCODE'']):\n        dvpc = int(row[''DVPORTCODE''] / 1000)\n    if dvpc == 372:\n        Irish = 1\n    elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):\n        if((row[''FLOW''] in (1,3))and (row[''RESIDENCE''] == 372)):\n            Irish = 1\n        elif((row[''FLOW''] in (2,4))and (row[''COUNTRYVISIT''] == 372)):\n            Irish = 1\n    if dvpc == 833:\n        IoM = 1\n    elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):\n        if((row[''FLOW''] in (1,3))and (row[''RESIDENCE''] == 833)):\n            IoM = 1\n        elif((row[''FLOW''] in (2,4))and (row[''COUNTRYVISIT''] == 833)):\n            IoM = 1\n    if dvpc in (831,832,931):\n        ChannelI = 1\n    elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):\n        if((row[''FLOW''] in (1,3))and (row[''RESIDENCE''] in (831,832,931))):\n            ChannelI = 1\n        elif((row[''FLOW''] in (2,4))and (row[''COUNTRYVISIT''] in (831,832,931))):\n            ChannelI = 1\nelif dataset == ''traffic'':\n    if row[''HAUL''] == ''E'':\n        Irish = 1\n    elif(row[''PORTROUTE''] == 250)or (row[''PORTROUTE''] == 350):\n        ChannelI = 1\n    elif(row[''PORTROUTE''] == 260)or (row[''PORTROUTE''] == 360):\n        IoM = 1\nif(Irish)and row[''PORTROUTE''] in (111,121,131,141,132,142,119,129,161,162,163,164,165,151,152,171,173,174,175):\n    row[''SAMP_PORT_GRP_PV''] = ''AHE''\nelif(Irish)and row[''PORTROUTE''] in (181,191,192,189,199):\n    row[''SAMP_PORT_GRP_PV''] = ''AGE''\nelif(Irish)and row[''PORTROUTE''] in (211,221,231,219):\n    row[''SAMP_PORT_GRP_PV''] = ''AME''\nelif(Irish)and row[''PORTROUTE''] in (241,249):\n    row[''SAMP_PORT_GRP_PV''] = ''ALE''\nelif(Irish)and row[''PORTROUTE''] in (201,202,203,204):\n    row[''SAMP_PORT_GRP_PV''] = ''ASE''\nelif(Irish)and (row[''PORTROUTE''] >= 300)and (row[''PORTROUTE''] < 600):\n    row[''SAMP_PORT_GRP_PV''] = ''ARE''\nelif(ChannelI)and (row[''PORTROUTE''] >= 100)and (row[''PORTROUTE''] < 300):\n    row[''SAMP_PORT_GRP_PV''] = ''MAC''\nelif(ChannelI)and (row[''PORTROUTE''] >= 300)and (row[''PORTROUTE''] < 600):\n    row[''SAMP_PORT_GRP_PV''] = ''RAC''\nelif(IoM)and (row[''PORTROUTE''] >= 100)and (row[''PORTROUTE''] < 300):\n    row[''SAMP_PORT_GRP_PV''] = ''MAM''\nelif(IoM)and (row[''PORTROUTE''] >= 300)and (row[''PORTROUTE''] < 600):\n    row[''SAMP_PORT_GRP_PV''] = ''RAM''\nif row[''SAMP_PORT_GRP_PV''] == ''HGS'':\n    row[''SAMP_PORT_GRP_PV''] = ''HBN''\nif row[''SAMP_PORT_GRP_PV''] == ''EGS'':\n    row[''SAMP_PORT_GRP_PV''] = ''HBN''\nif row[''SAMP_PORT_GRP_PV''] == ''MAM'':\n    row[''SAMP_PORT_GRP_PV''] = ''MAC''\nif row[''SAMP_PORT_GRP_PV''] == ''RAM'':\n    row[''SAMP_PORT_GRP_PV''] = ''RAC''');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 57, 'unsamp_port_grp_pv', 'unsamp_port_grp_pv               ',
        'if row[''PORTROUTE''] in (111,113,119,161):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A111'' \nelif row[''PORTROUTE''] in (121,123,129,162,172):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A121'' \nelif row[''PORTROUTE''] in (131,132,133,134,135,163,173):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A131'' \nelif row[''PORTROUTE''] in (141,142,143,144,145,164):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A141'' \nelif row[''PORTROUTE''] in (151,152,153,165):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A151'' \nelif row[''PORTROUTE''] in (181,183,189):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A181'' \nelif row[''PORTROUTE''] in (191,192,193,199):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A191'' \nelif row[''PORTROUTE''] in (201,202,203,204):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A201'' \nelif row[''PORTROUTE''] in (211,213,219):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A211'' \nelif row[''PORTROUTE''] in (221,223):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A221'' \nelif row[''PORTROUTE''] in (231,232,233,234):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A231'' \nelif row[''PORTROUTE''] in (241,243):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A241'' \nelif row[''PORTROUTE''] in (381,382,391,341,331,451):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A991'' \nelif row[''PORTROUTE''] in (401,411,441,443,471):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A992'' \nelif row[''PORTROUTE''] in (311,313,371,421,321,319):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A993'' \nelif row[''PORTROUTE''] in (461,351,361,481):\n    row[''UNSAMP_PORT_GRP_PV''] = ''A994'' \nelif row[''PORTROUTE''] == 991:\n    row[''UNSAMP_PORT_GRP_PV''] = ''A991'' \nelif row[''PORTROUTE''] == 992:\n    row[''UNSAMP_PORT_GRP_PV''] = ''A992'' \nelif row[''PORTROUTE''] == 993:\n    row[''UNSAMP_PORT_GRP_PV''] = ''A993'' \nelif row[''PORTROUTE''] == 994:\n    row[''UNSAMP_PORT_GRP_PV''] = ''A994'' \nelif row[''PORTROUTE''] == 995:\n    row[''UNSAMP_PORT_GRP_PV''] = ''ARE'' \nelif row[''PORTROUTE''] in (611,612,613,851,853,868,852):\n    row[''UNSAMP_PORT_GRP_PV''] = ''DCF'' \nelif row[''PORTROUTE''] in (621,631,632,633,634,854):\n    row[''UNSAMP_PORT_GRP_PV''] = ''SCF'' \nelif row[''PORTROUTE''] in (641,865):\n    row[''UNSAMP_PORT_GRP_PV''] = ''LHS'' \nelif row[''PORTROUTE''] in (635,636,651,652,661,662,856):\n    row[''UNSAMP_PORT_GRP_PV''] = ''SLR'' \nelif row[''PORTROUTE''] in (671,859,860,855):\n    row[''UNSAMP_PORT_GRP_PV''] = ''HBN'' \nelif row[''PORTROUTE''] in (672,858):\n    row[''UNSAMP_PORT_GRP_PV''] = ''HGS'' \nelif row[''PORTROUTE''] in (681,682,691,692,862):\n    row[''UNSAMP_PORT_GRP_PV''] = ''EGS'' \nelif row[''PORTROUTE''] in (701,711,741,864):\n    row[''UNSAMP_PORT_GRP_PV''] = ''SSE'' \nelif row[''PORTROUTE''] in (721,722,863):\n    row[''UNSAMP_PORT_GRP_PV''] = ''SNE'' \nelif row[''PORTROUTE''] in (731,861):\n    row[''UNSAMP_PORT_GRP_PV''] = ''RSS'' \nelif row[''PORTROUTE''] == 811:\n    row[''UNSAMP_PORT_GRP_PV''] = ''T811'' \nelif row[''PORTROUTE''] == 812:\n    row[''UNSAMP_PORT_GRP_PV''] = ''T812'' \nelif row[''PORTROUTE''] == 911:\n    row[''UNSAMP_PORT_GRP_PV''] = ''E911'' \nelif row[''PORTROUTE''] == 921:\n    row[''UNSAMP_PORT_GRP_PV''] = ''E921'' \nelif row[''PORTROUTE''] == 951:\n    row[''UNSAMP_PORT_GRP_PV''] = ''E951'' \nIrish = 0 \nIoM = 0 \nChannelI = 0 \ndvpc = 0 \nif dataset == ''survey'':\n    if not math.isnan(row[''DVPORTCODE'']):\n        dvpc = int(row[''DVPORTCODE''] / 1000)\n    if dvpc == 372:\n        Irish = 1 \n    elif(row[''DVPORTCODE''] == 999999)or math.isnan(row[''DVPORTCODE'']):\n        if((row[''FLOW''] in (1,3))and (row[''RESIDENCE''] == 372)):\n            Irish = 1 \n        elif((row[''FLOW''] in (2,4))and (row[''COUNTRYVISIT''] == 372)):\n            Irish = 1 \n    if dvpc == 833:\n        IoM = 1 \n    elif(row[''DVPORTCODE''] == 999999)or math.isnan(row[''DVPORTCODE'']):\n        if((row[''FLOW''] in (1,3))and (row[''RESIDENCE''] == 833)):\n            IoM = 1 \n        elif((row[''FLOW''] in (2,4))and (row[''COUNTRYVISIT''] == 833)):\n            IoM = 1 \n    if dvpc in (831,832,931):\n        ChannelI = 1 \n    elif(row[''DVPORTCODE''] == 999999)or math.isnan(row[''DVPORTCODE'']):\n        if((row[''FLOW''] in (1,3))and (row[''RESIDENCE''] in (831,832,931))):\n            ChannelI = 1 \n        elif((row[''FLOW''] in (2,4))and (row[''COUNTRYVISIT''] in (831,832,931))):\n            ChannelI = 1 \n    if(Irish)and row[''PORTROUTE''] in (111,121,129,131,141,132,142,119,161,162,163,164,165,151,152):\n        row[''UNSAMP_PORT_GRP_PV''] = ''AHE'' \n    elif(Irish)and row[''PORTROUTE''] in (181,191,192,189,199):\n        row[''UNSAMP_PORT_GRP_PV''] = ''AGE'' \n    elif(Irish)and row[''PORTROUTE''] in (211,221,231,219,249):\n        row[''UNSAMP_PORT_GRP_PV''] = ''AME'' \n    elif(Irish)and row[''PORTROUTE''] == 241:\n        row[''UNSAMP_PORT_GRP_PV''] = ''ALE'' \n    elif(Irish)and row[''PORTROUTE''] in (201,202):\n        row[''UNSAMP_PORT_GRP_PV''] = ''ASE'' \n    elif(Irish)and (row[''PORTROUTE''] >= 300)and (row[''PORTROUTE''] < 600):\n        row[''UNSAMP_PORT_GRP_PV''] = ''ARE'' \n    elif(ChannelI)and (row[''PORTROUTE''] >= 100)and (row[''PORTROUTE''] < 300):\n        row[''UNSAMP_PORT_GRP_PV''] = ''MAC'' \n    elif(ChannelI)and (row[''PORTROUTE''] >= 300)and (row[''PORTROUTE''] < 600):\n        row[''UNSAMP_PORT_GRP_PV''] = ''RAC'' \n    elif(IoM)and (row[''PORTROUTE''] >= 100)and (row[''PORTROUTE''] < 300):\n        row[''UNSAMP_PORT_GRP_PV''] = ''MAM'' \n    elif(IoM)and (row[''PORTROUTE''] >= 300)and (row[''PORTROUTE''] < 600):\n        row[''UNSAMP_PORT_GRP_PV''] = ''RAM''');


INSERT INTO `PROCESS_VARIABLE_PY` (`RUN_ID`, `PROCESS_VARIABLE_ID`, `PV_NAME`, `PV_DESC`, `PV_DEF`)
VALUES ('TEMPLATE', 58, 'unsamp_region_grp_pv', 'unsamp_region_grp_pv               ',
        'dvpc = 0\nrow[''ARRIVEDEPART''] = int(row[''ARRIVEDEPART''])\nif dataset == ''survey'':\n    if not math.isnan(row[''DVPORTCODE'']):\n        dvpc = int(row[''DVPORTCODE''] / 1000)\n    if row[''PORTROUTE''] < 300:\n        if row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):\n            if row[''FLOW''] in (1,3):\n                row[''REGION''] = row[''RESIDENCE'']\n            elif row[''FLOW''] in (2,4):\n                row[''REGION''] = row[''COUNTRYVISIT'']\n            else:\n                row[''REGION''] = ''''\n        else:\n            row[''REGION''] = dvpc\n        if row[''REGION''] in (8,20,31,40,51,56,70,100,112,191,203,208,233,234,246,250,268,276,348,352,380,398,417,428,440,442,492,498,499,528,578,616,642,643,674,688,703,705,752,756,762,795,804,807,860,940,942,943,944,945,946,950,951):\n            row[''UNSAMP_REGION_GRP_PV''] = 1.0\n        elif row[''REGION''] in (124,304,630,666,840,850):\n            row[''UNSAMP_REGION_GRP_PV''] = 2.0\n        elif row[''REGION''] in (4,36,50,64,96,104,116,126,144,156,158,242,356,360,408,410,418,446,458,462,496,524,554,586,608,626,702,704,764):\n            row[''UNSAMP_REGION_GRP_PV''] = 3.0\n        elif row[''REGION''] in (12,24,48,72,108,120,132,140,148,174,178,180,204,226,231,232,262,266,270,288,324,348,384,404,426,430,434,450,454,466,478,480,504,508,516,562,566,624,646,654,678,686,690,694,706,710,716,732,736,748,768,788,800,818,834,854,894):\n            row[''UNSAMP_REGION_GRP_PV''] = 4.0\n        elif row[''REGION''] == 392:\n            row[''UNSAMP_REGION_GRP_PV''] = 5.0\n        elif row[''REGION''] == 344:\n            row[''UNSAMP_REGION_GRP_PV''] = 6.0\n        elif row[''REGION''] in (16,28,32,44,48,52,60,68,76,84,90,92,136,152,166,170,184,188,192,212,214,218,222,238,254,258,296,308,312,316,320,328,332,340,364,368,376,388,400,414,422,474,484,500,512,520,530,533,540,548,558,580,581,584,591,598,604,634,638,659,660,662,670,682,690,740,760,776,780,784,796,798,858,862,882,887,949):\n            row[''UNSAMP_REGION_GRP_PV''] = 7.0\n        elif row[''REGION''] == 300:\n            row[''UNSAMP_REGION_GRP_PV''] = 8.0\n        elif row[''REGION''] in (292,620,621,911,912):\n            row[''UNSAMP_REGION_GRP_PV''] = 9.0\n        elif row[''REGION''] in (470,792,901,902):\n            row[''UNSAMP_REGION_GRP_PV''] = 10.0\n        elif row[''REGION''] == 372:\n            row[''UNSAMP_REGION_GRP_PV''] = 11.0\n        elif row[''REGION''] in (831,832,833,931):\n            row[''UNSAMP_REGION_GRP_PV''] = 12.0\n        elif row[''REGION''] in (921,923,924,926,933):\n            row[''UNSAMP_REGION_GRP_PV''] = 13.0\nelif dataset == ''unsampled'':\n    if not math.isnan(row[''REGION'']):\n        row[''REGION''] = int(row[''REGION''])\n        row[''UNSAMP_REGION_GRP_PV''] = row[''REGION'']\nif row[''UNSAMP_PORT_GRP_PV''] == ''A201'' and row[''UNSAMP_REGION_GRP_PV''] == 7.0 and row[''ARRIVEDEPART''] == 2:\n    row[''UNSAMP_PORT_GRP_PV''] = ''A191''\nif row[''UNSAMP_PORT_GRP_PV''] == ''HGS'':\n    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''\nif row[''UNSAMP_PORT_GRP_PV''] == ''E921'':\n    row[''UNSAMP_PORT_GRP_PV''] = ''E911''\nif row[''UNSAMP_PORT_GRP_PV''] == ''E951'':\n    row[''UNSAMP_PORT_GRP_PV''] = ''E911''');


INSERT INTO ips.PROCESS_VARIABLE_SET(run_id, name, user, period, year)
VALUES ('TEMPLATE', 'TEMPLATE', 'TEMPLATE', 'Q2', 2019);


INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (1, 'poweld2', 'pbkdf2:sha256:150000$sZ5Rjunm$9d0168de2dc9e3151e11ab43736168e89b81055911e44fc3112a29ba9f948f35',
        'David', 'Powell', 'user');


INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (2, 'Blakee', 'pbkdf2:sha256:150000$J6lntuGf$d53e139359f2ea7720395f425220e4cd4a92432dfa416f1f37f1ee65c62e373d',
        'Elliot', 'Blake', 'user');


INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (3, 'Lloydk1', 'pbkdf2:sha256:150000$cafn9zrq$133e002e1581e465b14cc1ec809047ba0934fe1d543f1ff32b7fc3e51a81807c',
        'Kimberley', 'Lloyd', 'user');

INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (4, 'lawe', 'pbkdf2:sha256:150000$LWO3LxIx$ad4f242884e8f74a8d5023f85afaee99ea44fa5d458375bf970ae2f2ae4c5908',
        'Eleanor', 'Law', 'user');

INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (5, 'thorne1', 'pbkdf2:sha256:150000$lYdI7i13$962b8271d2297c5b07929d8df719f8503c34a1a7a901759b13d532aef0882557',
        'Elinor', 'Thorne', 'user');

INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (6, 'urquh1', 'pbkdf2:sha256:150000$j41EINXU$7661aa0b958babe1c72e2c69689f962ac800c4d26696cab31758a86f6d09dde8',
        'Andrew', 'Urquhart', 'user');

INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (7, 'fear1', 'pbkdf2:sha256:150000$NbaMvbiQ$e2fa571febff5281ba87d97c5c8e6e688a00c50327cfff5de7df6d04d1828ccd',
        'Tara', 'Fear', 'user');

INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (8, 'kemps1', 'pbkdf2:sha256:150000$z3hdS8ce$e12471b8b0cb9f4214b0177fc32db970b67426049a2d35b04917b05aa8b6b0b9',
        'Mark', 'Kempson', 'user');
        
INSERT INTO USER (ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
VALUES (9, 'parker1', 'pbkdf2:sha256:150000$J03pGhox$c1967d65e68fbb35148552e982e3e1a7013520a6ea8a2b3e64104b66fcff4ba3',
        'Louise', 'Parker', 'user');

SET FOREIGN_KEY_CHECKS = 1;
