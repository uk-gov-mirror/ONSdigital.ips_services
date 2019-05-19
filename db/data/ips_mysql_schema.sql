create schema ips collate utf8mb4_0900_ai_ci;

create table AUDIT_LOG
(
	AUDIT_ID decimal not null,
	ACTIONED_BY varchar(20) not null,
	ACTION varchar(30) not null,
	OBJECT varchar(100) not null,
	LOG_DATE date not null,
	AUDIT_LOG_DETAILS varchar(1000) not null
);

create table COLUMN_LOOKUP
(
	LOOKUP_COLUMN varchar(50) not null,
	LOOKUP_KEY decimal(2) not null,
	DISPLAY_VALUE varchar(100) not null
);

create table DATA_SOURCE
(
	DATA_SOURCE_ID decimal not null
		primary key,
	DATA_SOURCE_NAME varchar(30) not null
);

create table DELTAS
(
	DELTA_NUMBER decimal(38) not null,
	RUN_DATE date not null,
	BACKOUT_DATE date null,
	DESCRIPTION varchar(100) not null
);

create table EXPORT_COLUMN
(
	EXPORT_TYPE_ID decimal not null,
	COLUMN_SOURCE varchar(3) not null,
	COLUMN_ORDER_NO decimal(4) not null,
	COLUMN_DESC varchar(30) not null,
	COLUMN_TYPE varchar(20) null,
	COLUMN_LENGTH decimal(38) null
);

create table EXPORT_DATA_DETAILS
(
	ED_ID varchar(40) not null,
	ED_NAME varchar(30) not null,
	EXPORT_TYPE_ID decimal not null,
	FORMAT_ID decimal not null,
	DATE_CREATED date not null,
	ED_STATUS decimal(2) not null,
	USER_ID varchar(20) not null
);

create table EXPORT_DATA_DOWNLOAD
(
	RUN_ID varchar(40) not null,
	DOWNLOADABLE_DATA longtext null,
	FILENAME varchar(40) null,
	SOURCE_TABLE varchar(40) null,
	DATE_CREATED datetime null
);

create table EXPORT_TYPE
(
	EXPORT_TYPE_ID decimal not null,
	EXPORT_TYPE_NAME varchar(30) not null,
	EXPORT_TYPE_DEF text not null
);

create table FORMAT_TYPE
(
	FORMAT_ID decimal not null,
	FORMAT_NAME varchar(30) not null,
	FORMAT_DEF varchar(2000) not null
);

create table G_PVs
(
	PV_ID int not null
		primary key,
	Name varchar(255) not null
);

create table G_PV_Variables
(
	PV_Variable_ID int not null
		primary key,
	PV_ID int not null,
	Name varchar(255) not null,
	constraint FK__G_PV_Vari__PV_ID__2739D489
		foreign key (PV_ID) references G_PVs (PV_ID)
			on update cascade on delete cascade
);

create table NON_RESPONSE_DATA
(
	RUN_ID varchar(40) not null,
	YEAR decimal(4) not null,
	MONTH decimal(2) not null,
	DATA_SOURCE_ID decimal not null,
	PORTROUTE decimal(4) not null,
	WEEKDAY decimal(1) null,
	ARRIVEDEPART decimal(1) null,
	AM_PM_NIGHT decimal(1) null,
	SAMPINTERVAL decimal(4) null,
	MIGTOTAL decimal null,
	ORDTOTAL decimal null
);

create table POPROWVEC_TRAFFIC
(
	C_GROUP bigint null,
	T_1 double null,
	T_2 double null,
	T_3 double null,
	T_4 double null,
	T_5 double null,
	T_6 double null,
	T_7 double null,
	T_8 double null,
	T_9 double null,
	T_10 double null,
	T_11 double null,
	T_12 double null,
	T_13 double null,
	T_14 double null,
	T_15 double null,
	T_16 double null,
	T_17 double null,
	T_18 double null,
	T_19 double null,
	T_20 double null,
	T_21 double null,
	T_22 double null,
	T_23 double null,
	T_24 double null,
	T_25 double null,
	T_26 double null,
	T_27 double null,
	T_28 double null,
	T_29 double null,
	T_30 double null,
	T_31 double null,
	T_32 double null,
	T_33 double null,
	T_34 double null,
	T_35 double null,
	T_36 double null,
	T_37 double null,
	T_38 double null,
	T_39 double null,
	T_40 double null,
	T_41 double null,
	T_42 double null,
	T_43 double null,
	T_44 double null,
	T_45 double null,
	T_46 double null,
	T_47 double null,
	T_48 double null,
	T_49 double null,
	T_50 double null,
	T_51 double null,
	T_52 double null,
	T_53 double null,
	T_54 double null,
	T_55 double null,
	T_56 double null,
	T_57 double null,
	T_58 double null,
	T_59 double null,
	T_60 double null,
	T_61 double null,
	T_62 double null,
	T_63 double null,
	T_64 double null,
	T_65 double null,
	T_66 double null,
	T_67 double null,
	T_68 double null,
	T_69 double null,
	T_70 double null,
	T_71 double null,
	T_72 double null,
	T_73 double null,
	T_74 double null,
	T_75 double null,
	T_76 double null,
	T_77 double null
);

create table POPROWVEC_UNSAMP
(
	T_1 double null,
	T_2 double null,
	T_3 double null,
	T_4 double null,
	T_5 double null,
	T_6 double null,
	T_7 double null,
	T_8 double null,
	T_9 double null,
	T_10 double null,
	T_11 double null,
	T_12 double null,
	T_13 double null,
	T_14 double null,
	T_15 double null,
	T_16 double null,
	T_17 double null,
	T_18 double null,
	T_19 double null,
	T_20 double null,
	T_21 double null,
	T_22 double null,
	T_23 double null,
	T_24 double null,
	T_25 double null,
	T_26 double null,
	T_27 double null,
	T_28 double null,
	T_29 double null,
	T_30 double null,
	T_31 double null,
	T_32 double null,
	T_33 double null,
	T_34 double null,
	T_35 double null,
	T_36 double null,
	T_37 double null,
	T_38 double null,
	T_39 double null,
	T_40 double null,
	T_41 double null,
	T_42 double null,
	T_43 double null,
	T_44 double null,
	T_45 double null,
	T_46 double null,
	T_47 double null,
	T_48 double null,
	T_49 double null,
	T_50 double null,
	T_51 double null,
	T_52 double null,
	T_53 double null,
	T_54 double null,
	T_55 double null,
	T_56 double null,
	T_57 double null,
	T_58 double null,
	T_59 double null,
	T_60 double null,
	T_61 double null,
	T_62 double null,
	T_63 double null,
	T_64 double null,
	T_65 double null,
	T_66 double null,
	T_67 double null,
	T_68 double null,
	T_69 double null,
	T_70 double null,
	T_71 double null,
	T_72 double null,
	T_73 double null,
	T_74 double null,
	T_75 double null,
	T_76 double null,
	T_77 double null,
	T_78 double null,
	T_79 double null,
	T_80 double null,
	T_81 double null,
	T_82 double null,
	T_83 double null,
	T_84 double null,
	T_85 double null,
	T_86 double null,
	T_87 double null,
	T_88 double null,
	T_89 double null,
	T_90 double null,
	T_91 double null,
	T_92 double null,
	T_93 double null,
	T_94 double null,
	T_95 double null,
	T_96 double null,
	T_97 double null,
	T_98 double null,
	T_99 double null,
	T_100 double null,
	T_101 double null,
	T_102 double null,
	T_103 double null,
	T_104 double null,
	T_105 double null,
	T_106 double null,
	T_107 double null,
	T_108 double null,
	T_109 double null,
	T_110 double null,
	T_111 double null,
	T_112 double null,
	T_113 double null,
	T_114 double null,
	T_115 double null,
	T_116 double null,
	T_117 double null,
	T_118 double null,
	T_119 double null,
	T_120 double null,
	T_121 double null,
	T_122 double null,
	T_123 double null,
	T_124 double null,
	T_125 double null,
	T_126 double null,
	T_127 double null,
	T_128 double null,
	T_129 double null,
	T_130 double null,
	T_131 double null,
	T_132 double null,
	T_133 double null,
	T_134 double null,
	T_135 double null,
	T_136 double null,
	T_137 double null,
	T_138 double null,
	T_139 double null,
	T_140 double null,
	T_141 double null,
	T_142 double null,
	T_143 double null,
	T_144 double null,
	T_145 double null,
	T_146 double null,
	T_147 double null,
	T_148 double null,
	T_149 double null,
	T_150 double null,
	T_151 double null,
	T_152 double null,
	T_153 double null,
	T_154 double null,
	T_155 double null,
	T_156 double null,
	T_157 double null,
	T_158 double null,
	T_159 double null,
	T_160 double null,
	T_161 double null,
	T_162 double null,
	T_163 double null,
	T_164 double null,
	T_165 double null,
	T_166 double null,
	T_167 double null,
	T_168 double null,
	T_169 double null,
	T_170 double null,
	T_171 double null,
	T_172 double null,
	T_173 double null,
	T_174 double null,
	T_175 double null,
	T_176 double null,
	T_177 double null,
	T_178 double null,
	T_179 double null,
	T_180 double null,
	T_181 double null,
	T_182 double null,
	T_183 double null,
	T_184 double null,
	T_185 double null,
	T_186 double null,
	T_187 double null,
	T_188 double null,
	T_189 double null,
	T_190 double null,
	T_191 double null,
	T_192 double null,
	T_193 double null,
	T_194 double null,
	T_195 double null,
	T_196 double null,
	T_197 double null,
	T_198 double null,
	T_199 double null,
	T_200 double null,
	T_201 double null,
	T_202 double null,
	T_203 double null
);

create table PROCESS_NAME
(
	PN_ID decimal not null,
	PROCESS_NAME varchar(30) not null
);

create table PROCESS_VARIABLE
(
	RUN_ID varchar(40) not null,
	PROCESS_VARIABLE_ID decimal not null,
	PV_NAME varchar(30) not null,
	PV_DESC varchar(1000) not null,
	PV_DEF text not null
);

create table PROCESS_VARIABLE_BACKUP
(
	RUN_ID varchar(40) not null,
	PROCESS_VARIABLE_ID decimal not null,
	PV_NAME varchar(30) not null,
	PV_DESC varchar(1000) not null,
	PV_DEF text not null
);

create table PROCESS_VARIABLE_LOG
(
	PROCESS_VARIABLE_ID decimal not null,
	PVL_DATE date not null,
	PVL_REASON varchar(1000) not null
);

create table PROCESS_VARIABLE_PY
(
	RUN_ID varchar(40) not null,
	PROCESS_VARIABLE_ID decimal not null,
	PV_NAME varchar(30) not null,
	PV_DESC varchar(1000) not null,
	PV_DEF text not null
);

create table PROCESS_VARIABLE_PY_BACKUP
(
	RUN_ID varchar(40) not null,
	PROCESS_VARIABLE_ID decimal not null,
	PV_NAME varchar(30) not null,
	PV_DESC varchar(1000) not null,
	PV_DEF text not null
);

create table PROCESS_VARIABLE_PY_BACKUP_2
(
	RUN_ID varchar(40) not null,
	PROCESS_VARIABLE_ID decimal not null,
	PV_NAME varchar(30) not null,
	PV_DESC varchar(1000) not null,
	PV_DEF text not null
);

create table PROCESS_VARIABLE_SET
(
	RUN_ID varchar(40) not null,
	NAME varchar(30) not null,
	USER varchar(50) null,
	PERIOD varchar(12) not null,
	YEAR year not null
);

create table PROCESS_VARIABLE_TESTING
(
	RUN_ID varchar(40) not null,
	PROCESS_VARIABLE_ID decimal not null,
	PV_NAME varchar(30) not null,
	PV_DESC varchar(1000) not null,
	PV_DEF text not null
);

create table PS_FINAL
(
	RUN_ID varchar(40) not null,
	SERIAL decimal(15) not null,
	SHIFT_WT decimal(9,3) null,
	NON_RESPONSE_WT decimal(9,3) null,
	MINS_WT decimal(9,3) null,
	TRAFFIC_WT decimal(9,3) null,
	UNSAMP_TRAFFIC_WT decimal(9,3) null,
	IMBAL_WT decimal(9,3) null,
	FINAL_WT decimal(12,3) null
);

create table PS_IMBALANCE
(
	RUN_ID varchar(40) not null,
	FLOW decimal(2) null,
	SUM_PRIOR_WT decimal(12,3) null,
	SUM_IMBAL_WT decimal(12,3) null
);

create table PS_INSTRUCTION
(
	PN_ID decimal not null,
	PS_INSTRUCTION varchar(2000) not null
);

create table PS_MINIMUMS
(
	RUN_ID varchar(40) not null,
	MINS_PORT_GRP_PV varchar(10) null,
	ARRIVEDEPART decimal(1) null,
	MINS_CTRY_GRP_PV decimal(6) null,
	MINS_NAT_GRP_PV decimal(6) null,
	MINS_CTRY_PORT_GRP_PV varchar(10) null,
	MINS_CASES decimal(6) null,
	FULLS_CASES decimal(6) null,
	PRIOR_GROSS_MINS decimal(12,3) null,
	PRIOR_GROSS_FULLS decimal(12,3) null,
	PRIOR_GROSS_ALL decimal(12,3) null,
	MINS_WT decimal(9,3) null,
	POST_SUM decimal(12,3) null,
	CASES_CARRIED_FWD decimal(6) null
);

create table PS_NON_RESPONSE
(
	RUN_ID varchar(40) not null,
	NR_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	WEEKDAY_END_PV decimal(1) null,
	MEAN_RESPS_SH_WT decimal(9,3) null,
	COUNT_RESPS decimal(6) null,
	PRIOR_SUM decimal(12,3) null,
	GROSS_RESP decimal(12,3) null,
	GNR decimal(12,3) null,
	MEAN_NR_WT decimal(9,3) null
);

create table PS_SHIFT_DATA
(
	RUN_ID varchar(40) not null,
	SHIFT_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	WEEKDAY_END_PV decimal(1) null,
	AM_PM_NIGHT_PV decimal(1) null,
	MIGSI int null,
	POSS_SHIFT_CROSS decimal(5) null,
	SAMP_SHIFT_CROSS decimal(5) null,
	MIN_SH_WT decimal(9,3) null,
	MEAN_SH_WT decimal(9,3) null,
	MAX_SH_WT decimal(9,3) null,
	COUNT_RESPS decimal(6) null,
	SUM_SH_WT decimal(12,3) null
);

create table PS_TRAFFIC
(
	RUN_ID varchar(40) not null,
	SAMP_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	FOOT_OR_VEHICLE_PV decimal(2) null,
	CASES decimal(6) null,
	TRAFFICTOTAL decimal(12,3) null,
	SUM_TRAFFIC_WT decimal(12,3) null,
	TRAFFIC_WT decimal(9,3) null
);

create table PS_UNSAMPLED_OOH
(
	RUN_ID varchar(40) not null,
	UNSAMP_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	UNSAMP_REGION_GRP_PV varchar(10) null,
	CASES decimal(6) null,
	SUM_PRIOR_WT decimal(12,3) null,
	SUM_UNSAMP_TRAFFIC_WT decimal(12,3) null,
	UNSAMP_TRAFFIC_WT decimal(9,3) null
);

create table PV_Block
(
	Block_ID int auto_increment
		primary key,
	Run_ID varchar(255) not null,
	Block_Index int not null,
	PV_ID int not null,
	constraint pv_fk
		foreign key (PV_ID) references G_PVs (PV_ID)
			on update cascade on delete cascade
);

create table PV_Expression
(
	Expression_ID int auto_increment
		primary key,
	Block_ID int not null,
	Expression_Index int not null,
	constraint block_fk
		foreign key (Block_ID) references PV_Block (Block_ID)
			on update cascade on delete cascade
);

create table PV_Element
(
	Element_ID int auto_increment
		primary key,
	Expression_ID int not null,
	type varchar(255) not null,
	content varchar(255) not null,
	constraint expression_fk
		foreign key (Expression_ID) references PV_Expression (Expression_ID)
			on update cascade on delete cascade
);

create table QUERY_RESPONSE
(
	TASK_ID varchar(40) not null,
	RESPONSE_CODE varchar(10) null,
	RESPONSE_MSG text null
);

create table RESPONSE
(
	RUN_ID varchar(40) not null,
	STEP_NUMBER int not null,
	RESPONSE_CODE int not null,
	MESSAGE varchar(250) null,
	OUTPUT varchar(4000) null,
	TIME_STAMP datetime null
);

create table RESPONSE_ARCHIVE
(
	RUN_ID varchar(40) not null,
	STEP_NUMBER int not null,
	RESPONSE_CODE int not null,
	MESSAGE varchar(250) null,
	OUTPUT varchar(4000) null,
	TIME_STAMP datetime null
);

create table RUN
(
	RUN_ID varchar(40) not null,
	RUN_NAME varchar(30) null,
	RUN_DESC varchar(250) null,
	USER_ID varchar(20) null,
	YEAR year null,
	PERIOD varchar(255) null,
	RUN_STATUS decimal(2) default 0 null,
	RUN_TYPE_ID decimal(3) default 0 null,
	LAST_MODIFIED timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
	STEP varchar(255) charset utf8 null,
	PERCENT int default 0 null,
	constraint RUN_RUN_ID_uindex
		unique (RUN_ID)
);

alter table RUN
	add primary key (RUN_ID);

create table RUN_DATA_MAP
(
	RUN_ID varchar(40) not null,
	VERSION_ID decimal not null,
	DATA_SOURCE varchar(60) not null,
	constraint RUN_DATA_MAP_RUN_ID_UINDEX
		unique (RUN_ID)
);

alter table RUN_DATA_MAP
	add primary key (RUN_ID);

create table RUN_STEPS
(
	RUN_ID varchar(40) not null,
	STEP_NUMBER decimal(2) not null,
	STEP_NAME varchar(80) not null,
	STEP_STATUS decimal(2) not null
);

create table RUN_TYPE
(
	RUN_TYPE_ID decimal(3) not null,
	RUN_TYPE_NAME varchar(30) not null,
	RUN_TYPE_DEFINITION text not null
);

create table R_TRAFFIC
(
	rownames varchar(255) null,
	SERIAL double null,
	ARRIVEDEPART int null,
	PORTROUTE int null,
	SAMP_PORT_GRP_PV varchar(255) null,
	SHIFT_WT float null,
	NON_RESPONSE_WT double null,
	MINS_WT double null,
	TRAFFIC_WT double null,
	TRAF_DESIGN_WEIGHT double null,
	T1 int null,
	T_ varchar(255) null,
	TW_WEIGHT double null
);

create table R_UNSAMPLED
(
	rownames varchar(255) null,
	SERIAL float null,
	ARRIVEDEPART int null,
	PORTROUTE int null,
	UNSAMP_PORT_GRP_PV varchar(255) null,
	UNSAMP_REGION_GRP_PV int null,
	SHIFT_WT float null,
	NON_RESPONSE_WT float null,
	MINS_WT float null,
	UNSAMP_TRAFFIC_WT float null,
	OOH_DESIGN_WEIGHT float null,
	T1 int null,
	T_ varchar(255) null,
	UNSAMP_TRAFFIC_WEIGHT float null
);

create table SAS_AIR_MILES
(
	SERIAL decimal(15) not null,
	DIRECTLEG decimal(6) null,
	OVLEG decimal(6) null,
	UKLEG decimal(6) null
);

create table SAS_DATA_EXPORT
(
	SAS_PROCESS_ID decimal not null,
	SDE_LABEL varchar(80) not null,
	SDE_DATA binary(1) not null
);

create table SAS_FARES_IMP
(
	SERIAL decimal(15) not null,
	FARE decimal(6) null,
	FAREK decimal(2) null,
	SPEND decimal(7) null,
	SPENDIMPREASON decimal(1) null
);

create table SAS_FARES_SPV
(
	SERIAL decimal(15) not null,
	FARES_IMP_FLAG_PV decimal(1) null,
	FARES_IMP_ELIGIBLE_PV decimal(1) null,
	DISCNT_PACKAGE_COST_PV decimal(6) null,
	DISCNT_F1_PV decimal(4,3) null,
	DISCNT_F2_PV decimal(4,3) null,
	FAGE_PV decimal(2) null,
	TYPE_PV decimal(2) null,
	OPERA_PV decimal(2) null,
	UKPORT1_PV decimal(4) null,
	UKPORT2_PV decimal(4) null,
	UKPORT3_PV decimal(4) null,
	UKPORT4_PV decimal(4) null,
	OSPORT1_PV decimal(8) null,
	OSPORT2_PV decimal(8) null,
	OSPORT3_PV decimal(8) null,
	OSPORT4_PV decimal(8) null,
	APD_PV decimal(4) null,
	QMFARE_PV decimal(8) null,
	DUTY_FREE_PV decimal(4) null
);

create table SAS_FINAL_WT
(
	SERIAL decimal(15) not null,
	FINAL_WT decimal(12,3) null
);

create table SAS_IMBALANCE_SPV
(
	SERIAL decimal(15) not null,
	IMBAL_PORT_GRP_PV decimal(3) null,
	IMBAL_PORT_SUBGRP_PV decimal(3) null,
	IMBAL_PORT_FACT_PV decimal(5,3) null,
	IMBAL_CTRY_GRP_PV decimal(3) null,
	IMBAL_CTRY_FACT_PV decimal(5,3) null,
	IMBAL_ELIGIBLE_PV decimal(1) null,
	PURPOSE_PV decimal(8) null,
	FLOW_PV decimal(2) null
);

create table SAS_IMBALANCE_WT
(
	SERIAL decimal(15) not null,
	IMBAL_WT decimal(9,3) null
);

create table SAS_MINIMUMS_SPV
(
	SERIAL decimal(15) not null,
	MINS_PORT_GRP_PV varchar(10) null,
	MINS_CTRY_GRP_PV decimal(6) null,
	MINS_NAT_GRP_PV decimal(6) null,
	MINS_CTRY_PORT_GRP_PV varchar(10) null,
	MINS_QUALITY_PV decimal(1) null,
	MINS_FLAG_PV decimal(1) null
);

create table SAS_MINIMUMS_WT
(
	SERIAL decimal(15) not null,
	MINS_WT decimal(9,3) null
);

create table SAS_NON_RESPONSE_DATA
(
	REC_ID int auto_increment
		primary key,
	PORTROUTE decimal(4) not null,
	WEEKDAY decimal(1) null,
	ARRIVEDEPART decimal(1) null,
	AM_PM_NIGHT decimal(1) null,
	SAMPINTERVAL decimal(4) null,
	MIGTOTAL decimal null,
	ORDTOTAL decimal null,
	NR_PORT_GRP_PV varchar(10) null,
	WEEKDAY_END_PV decimal(1) null,
	AM_PM_NIGHT_PV decimal(1) null
);

create table SAS_NON_RESPONSE_PV
(
	REC_ID decimal not null,
	WEEKDAY_END_PV decimal(1) null,
	NR_PORT_GRP_PV varchar(10) null
);

create table SAS_NON_RESPONSE_SPV
(
	SERIAL decimal(15) not null,
	NR_PORT_GRP_PV varchar(10) null,
	MIG_FLAG_PV decimal(1) null,
	NR_FLAG_PV decimal(1) null
);

create table SAS_NON_RESPONSE_WT
(
	SERIAL decimal(15) not null,
	NON_RESPONSE_WT decimal(9,3) null
);

create table SAS_PARAMETERS
(
	PARAMETER_SET_ID decimal not null,
	PARAMETER_NAME varchar(32) not null,
	PARAMETER_VALUE varchar(4000) null
);

create table SAS_PROCESS_VARIABLE
(
	PROCVAR_NAME varchar(30) not null,
	PROCVAR_RULE text not null,
	PROCVAR_ORDER decimal(2) not null
);

create table SAS_PS_FINAL
(
	SERIAL decimal(15) not null,
	SHIFT_WT decimal(9,3) null,
	NON_RESPONSE_WT decimal(9,3) null,
	MINS_WT decimal(9,3) null,
	TRAFFIC_WT decimal(9,3) null,
	UNSAMP_TRAFFIC_WT decimal(9,3) null,
	IMBAL_WT decimal(9,3) null,
	FINAL_WT decimal(12,3) null
);

create table SAS_PS_IMBALANCE
(
	FLOW decimal(2) null,
	SUM_PRIOR_WT decimal(12,3) null,
	SUM_IMBAL_WT decimal(12,3) null
);

create table SAS_PS_MINIMUMS
(
	MINS_PORT_GRP_PV varchar(10) null,
	ARRIVEDEPART decimal(1) null,
	MINS_CTRY_GRP_PV decimal(6) null,
	MINS_NAT_GRP_PV decimal(6) null,
	MINS_CTRY_PORT_GRP_PV varchar(10) null,
	MINS_CASES decimal(6) null,
	FULLS_CASES decimal(6) null,
	PRIOR_GROSS_MINS decimal(12,3) null,
	PRIOR_GROSS_FULLS decimal(12,3) null,
	PRIOR_GROSS_ALL decimal(12,3) null,
	MINS_WT decimal(9,3) null,
	POST_SUM decimal(12,3) null,
	CASES_CARRIED_FWD decimal(6) null
);

create table SAS_PS_NON_RESPONSE
(
	NR_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	WEEKDAY_END_PV decimal(1) null,
	MEAN_RESPS_SH_WT decimal(9,3) null,
	COUNT_RESPS decimal(6) null,
	PRIOR_SUM decimal(12,3) null,
	GROSS_RESP decimal(12,3) null,
	GNR decimal(12,3) null,
	MEAN_NR_WT decimal(9,3) null
);

create table SAS_PS_SHIFT_DATA
(
	SHIFT_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	WEEKDAY_END_PV decimal(1) null,
	AM_PM_NIGHT_PV decimal(1) null,
	MIGSI int null,
	POSS_SHIFT_CROSS decimal(5) null,
	SAMP_SHIFT_CROSS decimal(5) null,
	MIN_SH_WT decimal(9,3) null,
	MEAN_SH_WT decimal(9,3) null,
	MAX_SH_WT decimal(9,3) null,
	COUNT_RESPS decimal(6) null,
	SUM_SH_WT decimal(12,3) null
);

create table SAS_PS_TRAFFIC
(
	SAMP_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	FOOT_OR_VEHICLE_PV decimal(2) null,
	CASES decimal(6) null,
	TRAFFICTOTAL decimal(12,3) null,
	SUM_TRAFFIC_WT decimal(12,3) null,
	TRAFFIC_WT decimal(9,3) null
);

create table SAS_PS_UNSAMPLED_OOH
(
	UNSAMP_PORT_GRP_PV varchar(10) not null,
	ARRIVEDEPART decimal(1) not null,
	UNSAMP_REGION_GRP_PV varchar(10) null,
	CASES decimal(6) null,
	SUM_PRIOR_WT decimal(12,3) null,
	SUM_UNSAMP_TRAFFIC_WT decimal(12,3) null,
	UNSAMP_TRAFFIC_WT decimal(9,3) null
);

create table SAS_RAIL_IMP
(
	SERIAL decimal(15) not null,
	SPEND decimal(7) null
);

create table SAS_RAIL_SPV
(
	SERIAL decimal(15) not null,
	RAIL_CNTRY_GRP_PV decimal(3) null,
	RAIL_EXERCISE_PV decimal(6) null,
	RAIL_IMP_ELIGIBLE_PV decimal(1) null
);

create table SAS_REGIONAL_IMP
(
	SERIAL decimal(15) not null,
	VISIT_WT decimal(6,3) null,
	STAY_WT decimal(6,3) null,
	EXPENDITURE_WT decimal(6,3) null,
	VISIT_WTK varchar(10) null,
	STAY_WTK varchar(10) null,
	EXPENDITURE_WTK varchar(10) null,
	NIGHTS1 decimal(3) null,
	NIGHTS2 decimal(3) null,
	NIGHTS3 decimal(3) null,
	NIGHTS4 decimal(3) null,
	NIGHTS5 decimal(3) null,
	NIGHTS6 decimal(3) null,
	NIGHTS7 decimal(3) null,
	NIGHTS8 decimal(3) null,
	STAY1K varchar(10) null,
	STAY2K varchar(10) null,
	STAY3K varchar(10) null,
	STAY4K varchar(10) null,
	STAY5K varchar(10) null,
	STAY6K varchar(10) null,
	STAY7K varchar(10) null,
	STAY8K varchar(10) null
);

create table SAS_REGIONAL_SPV
(
	SERIAL decimal(15) not null,
	PURPOSE_PV decimal(8) null,
	STAYIMPCTRYLEVEL1_PV decimal(8) null,
	STAYIMPCTRYLEVEL2_PV decimal(8) null,
	STAYIMPCTRYLEVEL3_PV decimal(8) null,
	STAYIMPCTRYLEVEL4_PV decimal(8) null,
	REG_IMP_ELIGIBLE_PV decimal(1) null
);

create table SAS_RESPONSE
(
	SAS_PROCESS_ID decimal not null,
	RESPONSE_CODE decimal(5) not null,
	ERROR_MSG varchar(250) null,
	STACK_TRACE varchar(4000) null,
	WARNINGS varchar(4000) null,
	TIME_STAMP datetime null
);

create table SAS_SHIFT_DATA
(
	REC_ID int auto_increment
		primary key,
	PORTROUTE decimal(4) not null,
	WEEKDAY decimal(1) not null,
	ARRIVEDEPART decimal(1) not null,
	TOTAL decimal not null,
	AM_PM_NIGHT decimal(1) not null,
	SHIFT_PORT_GRP_PV varchar(10) null,
	AM_PM_NIGHT_PV decimal(1) null,
	WEEKDAY_END_PV decimal(1) null
);

create table SAS_SHIFT_DATA_RICER
(
	REC_ID decimal not null,
	PORTROUTE decimal(4) not null,
	WEEKDAY decimal(1) not null,
	ARRIVEDEPART decimal(1) not null,
	TOTAL decimal not null,
	AM_PM_NIGHT decimal(1) not null,
	SHIFT_PORT_GRP_PV varchar(10) null,
	AM_PM_NIGHT_PV decimal(1) null,
	WEEKDAY_END_PV decimal(1) null
);

create table SAS_SHIFT_PV
(
	REC_ID decimal not null,
	SHIFT_PORT_GRP_PV varchar(10) null,
	AM_PM_NIGHT_PV decimal(1) null,
	WEEKDAY_END_PV decimal(1) null
);

create table SAS_SHIFT_SPV
(
	SERIAL decimal(15) not null,
	SHIFT_PORT_GRP_PV varchar(10) null,
	AM_PM_NIGHT_PV decimal(1) null,
	WEEKDAY_END_PV decimal(1) null,
	SHIFT_FLAG_PV decimal(1) null,
	CROSSINGS_FLAG_PV decimal(1) null,
	constraint SAS_SHIFT_SPV_pk
		unique (SERIAL)
);

create table SAS_SHIFT_WT
(
	SERIAL decimal(15) not null,
	SHIFT_WT decimal(9,3) null
);

create table SAS_SPEND_IMP
(
	SERIAL decimal(15) not null
		primary key,
	NEWSPEND decimal(7) null,
	SPENDK decimal(2) null
);

create table SAS_SPEND_SPV
(
	SERIAL decimal(15) not null,
	SPEND_IMP_FLAG_PV decimal(1) null,
	SPEND_IMP_ELIGIBLE_PV decimal(1) null,
	UK_OS_PV decimal(2) null,
	PUR1_PV decimal(8) null,
	PUR2_PV decimal(8) null,
	PUR3_PV decimal(8) null,
	DUR1_PV decimal(8) null,
	DUR2_PV decimal(8) null
);

create table SAS_STAY_IMP
(
	SERIAL decimal(15) not null
		primary key,
	STAY decimal(3) null,
	STAYK decimal(1) null
);

create table SAS_STAY_SPV
(
	SERIAL decimal(15) not null,
	STAY_IMP_FLAG_PV decimal(1) null,
	STAY_IMP_ELIGIBLE_PV decimal(1) null,
	STAYIMPCTRYLEVEL1_PV decimal(8) null,
	STAYIMPCTRYLEVEL2_PV decimal(8) null,
	STAYIMPCTRYLEVEL3_PV decimal(8) null,
	STAYIMPCTRYLEVEL4_PV decimal(8) null,
	STAY_PURPOSE_GRP_PV decimal(2) null
);

create index SAS_STAY_SPV_SERIAL_index
	on SAS_STAY_SPV (SERIAL);

create table SAS_SURVEY_COLUMN
(
	VERSION_ID decimal not null,
	COLUMN_NO decimal(4) not null,
	COLUMN_DESC varchar(30) not null,
	COLUMN_TYPE varchar(20) not null,
	COLUMN_LENGTH decimal(5) not null
);

create table SAS_SURVEY_SUBSAMPLE
(
	SERIAL decimal(15) not null,
	AGE decimal(3) null,
	AM_PM_NIGHT decimal(1) null,
	ANYUNDER16 varchar(2) null,
	APORTLATDEG decimal(2) null,
	APORTLATMIN decimal(2) null,
	APORTLATSEC decimal(2) null,
	APORTLATNS varchar(1) null,
	APORTLONDEG decimal(3) null,
	APORTLONMIN decimal(2) null,
	APORTLONSEC decimal(2) null,
	APORTLONEW varchar(1) null,
	ARRIVEDEPART decimal(1) null,
	BABYFARE decimal(4,2) null,
	BEFAF decimal(6) null,
	CHANGECODE decimal(6) null,
	CHILDFARE decimal(4,2) null,
	COUNTRYVISIT decimal(4) null,
	CPORTLATDEG decimal(2) null,
	CPORTLATMIN decimal(2) null,
	CPORTLATSEC decimal(2) null,
	CPORTLATNS varchar(1) null,
	CPORTLONDEG decimal(3) null,
	CPORTLONMIN decimal(2) null,
	CPORTLONSEC decimal(2) null,
	CPORTLONEW varchar(1) null,
	INTDATE varchar(8) null,
	DAYTYPE decimal(1) null,
	DIRECTLEG decimal(6) null,
	DVEXPEND decimal(6) null,
	DVFARE decimal(6) null,
	DVLINECODE decimal(6) null,
	DVPACKAGE decimal(1) null,
	DVPACKCOST decimal(6) null,
	DVPERSONS decimal(3) null,
	DVPORTCODE decimal(6) null,
	EXPENDCODE varchar(4) null,
	EXPENDITURE decimal(6) null,
	FARE decimal(6) null,
	FAREK decimal(2) null,
	FLOW decimal(2) null,
	HAULKEY decimal(2) null,
	INTENDLOS decimal(2) null,
	KIDAGE decimal(2) null,
	LOSKEY decimal(2) null,
	MAINCONTRA decimal(1) null,
	MIGSI int null,
	INTMONTH decimal(2) null,
	NATIONALITY decimal(4) null,
	NATIONNAME varchar(50) null,
	NIGHTS1 decimal(3) null,
	NIGHTS2 decimal(3) null,
	NIGHTS3 decimal(3) null,
	NIGHTS4 decimal(3) null,
	NIGHTS5 decimal(3) null,
	NIGHTS6 decimal(3) null,
	NIGHTS7 decimal(3) null,
	NIGHTS8 decimal(3) null,
	NUMADULTS decimal(3) null,
	NUMDAYS decimal(3) null,
	NUMNIGHTS decimal(3) null,
	NUMPEOPLE decimal(3) null,
	PACKAGEHOL decimal(1) null,
	PACKAGEHOLUK decimal(1) null,
	PERSONS decimal(2) null,
	PORTROUTE decimal(4) null,
	PACKAGE decimal(2) null,
	PROUTELATDEG decimal(2) null,
	PROUTELATMIN decimal(2) null,
	PROUTELATSEC decimal(2) null,
	PROUTELATNS varchar(1) null,
	PROUTELONDEG decimal(3) null,
	PROUTELONMIN decimal(2) null,
	PROUTELONSEC decimal(2) null,
	PROUTELONEW varchar(1) null,
	PURPOSE decimal(2) null,
	QUARTER decimal(1) null,
	RESIDENCE decimal(4) null,
	RESPNSE decimal(2) null,
	SEX decimal(1) null,
	SHIFTNO decimal(6) null,
	SHUTTLE decimal(1) null,
	SINGLERETURN decimal(1) null,
	TANDTSI decimal(8) null,
	TICKETCOST decimal(6) null,
	TOWNCODE1 decimal(6) null,
	TOWNCODE2 decimal(6) null,
	TOWNCODE3 decimal(6) null,
	TOWNCODE4 decimal(6) null,
	TOWNCODE5 decimal(6) null,
	TOWNCODE6 decimal(6) null,
	TOWNCODE7 decimal(6) null,
	TOWNCODE8 decimal(6) null,
	TRANSFER decimal(6) null,
	UKFOREIGN decimal(1) null,
	VEHICLE decimal(1) null,
	VISITBEGAN varchar(8) null,
	WELSHNIGHTS decimal(3) null,
	WELSHTOWN decimal(6) null,
	AM_PM_NIGHT_PV decimal(1) null,
	APD_PV decimal(4) null,
	ARRIVEDEPART_PV decimal(1) null,
	CROSSINGS_FLAG_PV decimal(1) null,
	STAYIMPCTRYLEVEL1_PV decimal(8) null,
	STAYIMPCTRYLEVEL2_PV decimal(8) null,
	STAYIMPCTRYLEVEL3_PV decimal(8) null,
	STAYIMPCTRYLEVEL4_PV decimal(8) null,
	DAY_PV decimal(2) null,
	DISCNT_F1_PV decimal(4,3) null,
	DISCNT_F2_PV decimal(4,3) null,
	DISCNT_PACKAGE_COST_PV decimal(6) null,
	DUR1_PV decimal(3) null,
	DUR2_PV decimal(3) null,
	DUTY_FREE_PV decimal(4) null,
	FAGE_PV decimal(2) null,
	FARES_IMP_ELIGIBLE_PV decimal(1) null,
	FARES_IMP_FLAG_PV decimal(1) null,
	FLOW_PV decimal(2) null,
	FOOT_OR_VEHICLE_PV decimal(2) null,
	HAUL_PV varchar(2) null,
	IMBAL_CTRY_FACT_PV decimal(5,3) null,
	IMBAL_CTRY_GRP_PV decimal(3) null,
	IMBAL_ELIGIBLE_PV decimal(1) null,
	IMBAL_PORT_FACT_PV decimal(5,3) null,
	IMBAL_PORT_GRP_PV decimal(3) null,
	IMBAL_PORT_SUBGRP_PV decimal(3) null,
	LOS_PV decimal(3) null,
	LOSDAYS_PV decimal(3) null,
	MIG_FLAG_PV decimal(1) null,
	MINS_CTRY_GRP_PV decimal(6) null,
	MINS_CTRY_PORT_GRP_PV varchar(10) null,
	MINS_FLAG_PV decimal(1) null,
	MINS_NAT_GRP_PV decimal(6) null,
	MINS_PORT_GRP_PV varchar(6) null,
	MINS_QUALITY_PV decimal(1) null,
	NR_FLAG_PV decimal(1) null,
	NR_PORT_GRP_PV varchar(10) null,
	OPERA_PV decimal(2) null,
	OSPORT1_PV decimal(8) null,
	OSPORT2_PV decimal(8) null,
	OSPORT3_PV decimal(8) null,
	OSPORT4_PV decimal(8) null,
	PUR1_PV decimal(8) null,
	PUR2_PV decimal(8) null,
	PUR3_PV decimal(8) null,
	PURPOSE_PV decimal(8) null,
	QMFARE_PV decimal(8) null,
	RAIL_CNTRY_GRP_PV decimal(3) null,
	RAIL_EXERCISE_PV decimal(6) null,
	RAIL_IMP_ELIGIBLE_PV decimal(1) null,
	REG_IMP_ELIGIBLE_PV decimal(1) null,
	SAMP_PORT_GRP_PV varchar(10) null,
	SHIFT_FLAG_PV decimal(1) null,
	SHIFT_PORT_GRP_PV varchar(10) null,
	SPEND_IMP_FLAG_PV decimal(1) null,
	SPEND_IMP_ELIGIBLE_PV decimal(1) null,
	STAY_IMP_ELIGIBLE_PV decimal(1) null,
	STAY_IMP_FLAG_PV decimal(1) null,
	STAY_PURPOSE_GRP_PV decimal(2) null,
	TOWNCODE_PV varchar(10) null,
	TOWN_IMP_ELIGIBLE_PV decimal(1) null,
	TYPE_PV decimal(2) null,
	UK_OS_PV decimal(1) null,
	UKPORT1_PV decimal(4) null,
	UKPORT2_PV decimal(4) null,
	UKPORT3_PV decimal(4) null,
	UKPORT4_PV decimal(4) null,
	UNSAMP_PORT_GRP_PV varchar(10) null,
	UNSAMP_REGION_GRP_PV varchar(10) null,
	WEEKDAY_END_PV decimal(1) null,
	DIRECT decimal(6) null,
	EXPENDITURE_WT decimal(6,3) null,
	EXPENDITURE_WTK varchar(10) null,
	FAREKEY varchar(4) null,
	OVLEG decimal(6) null,
	SPEND decimal(7) null,
	SPEND1 decimal(7) null,
	SPEND2 decimal(7) null,
	SPEND3 decimal(7) null,
	SPEND4 decimal(7) null,
	SPEND5 decimal(7) null,
	SPEND6 decimal(7) null,
	SPEND7 decimal(7) null,
	SPEND8 decimal(7) null,
	SPEND9 decimal(7) null,
	SPENDIMPREASON decimal(1) null,
	SPENDK decimal(2) null,
	STAY decimal(3) null,
	STAYK decimal(1) null,
	STAY1K varchar(10) null,
	STAY2K varchar(10) null,
	STAY3K varchar(10) null,
	STAY4K varchar(10) null,
	STAY5K varchar(10) null,
	STAY6K varchar(10) null,
	STAY7K varchar(10) null,
	STAY8K varchar(10) null,
	STAY9K varchar(10) null,
	STAYTLY decimal(6) null,
	STAY_WT decimal(6,3) null,
	STAY_WTK varchar(10) null,
	TYPEINTERVIEW decimal(3) null,
	UKLEG decimal(6) null,
	VISIT_WT decimal(6,3) null,
	VISIT_WTK varchar(10) null,
	SHIFT_WT decimal(9,3) null,
	NON_RESPONSE_WT decimal(9,3) null,
	MINS_WT decimal(9,3) null,
	TRAFFIC_WT decimal(9,3) null,
	UNSAMP_TRAFFIC_WT decimal(9,3) null,
	IMBAL_WT decimal(9,3) null,
	FINAL_WT decimal(12,3) null,
	constraint SAS_SURVEY_SUBSAMPLE_pk
		unique (SERIAL)
);

create table SAS_SURVEY_VALUE
(
	VERSION_ID decimal not null,
	SERIAL_NO decimal(15) not null,
	COLUMN_NO decimal(4) not null,
	COLUMN_VALUE varchar(100) not null
);

create table SAS_TOWN_STAY_IMP
(
	SERIAL decimal(15) not null,
	SPEND1 decimal(7) null,
	SPEND2 decimal(7) null,
	SPEND3 decimal(7) null,
	SPEND4 decimal(7) null,
	SPEND5 decimal(7) null,
	SPEND6 decimal(7) null,
	SPEND7 decimal(7) null,
	SPEND8 decimal(7) null
);

create table SAS_TOWN_STAY_SPV
(
	SERIAL decimal(15) not null,
	PURPOSE_PV decimal(8) null,
	STAYIMPCTRYLEVEL1_PV decimal(8) null,
	STAYIMPCTRYLEVEL2_PV decimal(8) null,
	STAYIMPCTRYLEVEL3_PV decimal(8) null,
	STAYIMPCTRYLEVEL4_PV decimal(8) null,
	TOWN_IMP_ELIGIBLE_PV decimal(1) null
);

create table SAS_TRAFFIC_DATA
(
	REC_ID int auto_increment
		primary key,
	PORTROUTE decimal(4) null,
	ARRIVEDEPART decimal(1) null,
	TRAFFICTOTAL decimal null,
	PERIODSTART varchar(10) null,
	PERIODEND varchar(10) null,
	AM_PM_NIGHT decimal(1) null,
	HAUL varchar(2) null,
	VEHICLE decimal(1) null,
	SAMP_PORT_GRP_PV varchar(10) null,
	FOOT_OR_VEHICLE_PV decimal(2) null,
	HAUL_PV varchar(2) null
);

create table SAS_TRAFFIC_PV
(
	REC_ID decimal not null,
	SAMP_PORT_GRP_PV varchar(10) null,
	FOOT_OR_VEHICLE_PV decimal(2) null,
	HAUL_PV varchar(2) null
);

create table SAS_TRAFFIC_SPV
(
	SERIAL decimal(15) not null,
	SAMP_PORT_GRP_PV varchar(10) null,
	FOOT_OR_VEHICLE_PV decimal(2) null,
	HAUL_PV varchar(2) null
);

create table SAS_TRAFFIC_WT
(
	SERIAL decimal(15) not null,
	TRAFFIC_WT decimal(9,3) null
);

create table SAS_UNSAMPLED_OOH_DATA
(
	REC_ID int auto_increment
		primary key,
	PORTROUTE decimal(4) null,
	REGION decimal(3) null,
	ARRIVEDEPART decimal(1) null,
	UNSAMP_TOTAL decimal null,
	UNSAMP_PORT_GRP_PV varchar(10) null,
	UNSAMP_REGION_GRP_PV varchar(10) null
);

create table SAS_UNSAMPLED_OOH_PV
(
	REC_ID decimal not null,
	UNSAMP_PORT_GRP_PV varchar(10) not null,
	UNSAMP_REGION_GRP_PV varchar(10) null,
	HAUL_PV varchar(2) null
);

create table SAS_UNSAMPLED_OOH_SPV
(
	SERIAL decimal(15) not null,
	UNSAMP_PORT_GRP_PV varchar(10) null,
	UNSAMP_REGION_GRP_PV varchar(10) null,
	HAUL_PV varchar(2) null
);

create table SAS_UNSAMPLED_OOH_WT
(
	SERIAL decimal(15) not null,
	UNSAMP_TRAFFIC_WT decimal(9,3) null
);

create table SERIALISED_RUN
(
	RUN_ID varchar(40) not null,
	SER_OBJ binary(1) not null
);

create table SERIALISED_WORKFLOW
(
	WORKFLOW_ID varchar(40) not null,
	SER_OBJ binary(1) not null
);

create table SHIFT_DATA
(
	RUN_ID varchar(40) not null,
	YEAR decimal(4) not null,
	MONTH decimal(2) not null,
	DATA_SOURCE_ID decimal not null,
	PORTROUTE decimal(4) not null,
	WEEKDAY decimal(1) not null,
	ARRIVEDEPART decimal(1) not null,
	TOTAL decimal not null,
	AM_PM_NIGHT decimal(1) not null
);

create index SHIFT_DATA_RUN_ID_index
	on SHIFT_DATA (RUN_ID);

create table SPSS_METADATA
(
	NAME varchar(30) null,
	TYPE varchar(30) null,
	LENGTH decimal(3) null
);

create table SQL_QUERY
(
	TASK_ID varchar(40) not null,
	QUERY_STRING text not null,
	QUERY_MESSAGE varchar(4000) null
);

create table STATE_MAINTENANCE
(
	STATE_ID decimal not null,
	USER_ID varchar(20) null,
	WORKFLOW_ID decimal not null,
	ACTION varchar(30) null,
	OBJECT varchar(100) null,
	STATUS decimal(1) null,
	COMMENTS varchar(500) null
);

create table STEP
(
	STEP_ID varchar(40) not null,
	STEP_DEFINITION text not null,
	STEP_MESSAGE varchar(4000) null
);

create table SURVEY_COLUMN
(
	VERSION_ID decimal not null,
	COLUMN_NO decimal(4) not null,
	COLUMN_DESC varchar(30) not null,
	COLUMN_TYPE varchar(20) not null,
	COLUMN_LENGTH decimal(5) not null
);

create table SURVEY_SUBSAMPLE
(
	RUN_ID varchar(40) not null,
	SERIAL decimal(15) not null,
	AGE decimal(3) null,
	AM_PM_NIGHT decimal(1) null,
	ANYUNDER16 varchar(2) null,
	APORTLATDEG decimal(2) null,
	APORTLATMIN decimal(2) null,
	APORTLATSEC decimal(2) null,
	APORTLATNS varchar(1) null,
	APORTLONDEG decimal(3) null,
	APORTLONMIN decimal(2) null,
	APORTLONSEC decimal(2) null,
	APORTLONEW varchar(1) null,
	ARRIVEDEPART decimal(1) null,
	BABYFARE decimal(4,2) null,
	BEFAF decimal(6) null,
	CHANGECODE decimal(6) null,
	CHILDFARE decimal(4,2) null,
	COUNTRYVISIT decimal(4) null,
	CPORTLATDEG decimal(2) null,
	CPORTLATMIN decimal(2) null,
	CPORTLATSEC decimal(2) null,
	CPORTLATNS varchar(1) null,
	CPORTLONDEG decimal(3) null,
	CPORTLONMIN decimal(2) null,
	CPORTLONSEC decimal(2) null,
	CPORTLONEW varchar(1) null,
	INTDATE varchar(8) null,
	DAYTYPE decimal(1) null,
	DIRECTLEG decimal(6) null,
	DVEXPEND decimal(6) null,
	DVFARE decimal(6) null,
	DVLINECODE decimal(6) null,
	DVPACKAGE decimal(1) null,
	DVPACKCOST decimal(6) null,
	DVPERSONS decimal(3) null,
	DVPORTCODE decimal(6) null,
	EXPENDCODE varchar(4) null,
	EXPENDITURE decimal(6) null,
	FARE decimal(6) null,
	FAREK decimal(2) null,
	FLOW decimal(2) null,
	HAULKEY decimal(2) null,
	INTENDLOS decimal(2) null,
	KIDAGE decimal(2) null,
	LOSKEY decimal(2) null,
	MAINCONTRA decimal(1) null,
	MIGSI int null,
	INTMONTH decimal(2) null,
	NATIONALITY decimal(4) null,
	NATIONNAME varchar(50) null,
	NIGHTS1 decimal(3) null,
	NIGHTS2 decimal(3) null,
	NIGHTS3 decimal(3) null,
	NIGHTS4 decimal(3) null,
	NIGHTS5 decimal(3) null,
	NIGHTS6 decimal(3) null,
	NIGHTS7 decimal(3) null,
	NIGHTS8 decimal(3) null,
	NUMADULTS decimal(3) null,
	NUMDAYS decimal(3) null,
	NUMNIGHTS decimal(3) null,
	NUMPEOPLE decimal(3) null,
	PACKAGEHOL decimal(1) null,
	PACKAGEHOLUK decimal(1) null,
	PERSONS decimal(2) null,
	PORTROUTE decimal(4) null,
	PACKAGE decimal(2) null,
	PROUTELATDEG decimal(2) null,
	PROUTELATMIN decimal(2) null,
	PROUTELATSEC decimal(2) null,
	PROUTELATNS varchar(1) null,
	PROUTELONDEG decimal(3) null,
	PROUTELONMIN decimal(2) null,
	PROUTELONSEC decimal(2) null,
	PROUTELONEW varchar(1) null,
	PURPOSE decimal(2) null,
	QUARTER decimal(1) null,
	RESIDENCE decimal(4) null,
	RESPNSE decimal(2) null,
	SEX decimal(1) null,
	SHIFTNO decimal(6) null,
	SHUTTLE decimal(1) null,
	SINGLERETURN decimal(1) null,
	TANDTSI decimal(8) null,
	TICKETCOST decimal(6) null,
	TOWNCODE1 decimal(6) null,
	TOWNCODE2 decimal(6) null,
	TOWNCODE3 decimal(6) null,
	TOWNCODE4 decimal(6) null,
	TOWNCODE5 decimal(6) null,
	TOWNCODE6 decimal(6) null,
	TOWNCODE7 decimal(6) null,
	TOWNCODE8 decimal(6) null,
	TRANSFER decimal(6) null,
	UKFOREIGN decimal(1) null,
	VEHICLE decimal(1) null,
	VISITBEGAN varchar(8) null,
	WELSHNIGHTS decimal(3) null,
	WELSHTOWN decimal(6) null,
	AM_PM_NIGHT_PV decimal(1) null,
	APD_PV decimal(4) null,
	ARRIVEDEPART_PV decimal(1) null,
	CROSSINGS_FLAG_PV decimal(1) null,
	STAYIMPCTRYLEVEL1_PV decimal(8) null,
	STAYIMPCTRYLEVEL2_PV decimal(8) null,
	STAYIMPCTRYLEVEL3_PV decimal(8) null,
	STAYIMPCTRYLEVEL4_PV decimal(8) null,
	DAY_PV decimal(2) null,
	DISCNT_F1_PV decimal(4,3) null,
	DISCNT_F2_PV decimal(4,3) null,
	DISCNT_PACKAGE_COST_PV decimal(6) null,
	DUR1_PV decimal(3) null,
	DUR2_PV decimal(3) null,
	DUTY_FREE_PV decimal(4) null,
	FAGE_PV decimal(2) null,
	FARES_IMP_ELIGIBLE_PV decimal(1) null,
	FARES_IMP_FLAG_PV decimal(1) null,
	FLOW_PV decimal(2) null,
	FOOT_OR_VEHICLE_PV decimal(2) null,
	HAUL_PV varchar(2) null,
	IMBAL_CTRY_FACT_PV decimal(5,3) null,
	IMBAL_CTRY_GRP_PV decimal(3) null,
	IMBAL_ELIGIBLE_PV decimal(1) null,
	IMBAL_PORT_FACT_PV decimal(5,3) null,
	IMBAL_PORT_GRP_PV decimal(3) null,
	IMBAL_PORT_SUBGRP_PV decimal(3) null,
	LOS_PV decimal(3) null,
	LOSDAYS_PV decimal(3) null,
	MIG_FLAG_PV decimal(1) null,
	MINS_CTRY_GRP_PV decimal(6) null,
	MINS_CTRY_PORT_GRP_PV varchar(10) null,
	MINS_FLAG_PV decimal(1) null,
	MINS_NAT_GRP_PV decimal(6) null,
	MINS_PORT_GRP_PV varchar(6) null,
	MINS_QUALITY_PV decimal(1) null,
	NR_FLAG_PV decimal(1) null,
	NR_PORT_GRP_PV varchar(10) null,
	OPERA_PV decimal(2) null,
	OSPORT1_PV decimal(8) null,
	OSPORT2_PV decimal(8) null,
	OSPORT3_PV decimal(8) null,
	OSPORT4_PV decimal(8) null,
	PUR1_PV decimal(8) null,
	PUR2_PV decimal(8) null,
	PUR3_PV decimal(8) null,
	PURPOSE_PV decimal(8) null,
	QMFARE_PV decimal(8) null,
	RAIL_CNTRY_GRP_PV decimal(3) null,
	RAIL_EXERCISE_PV decimal(6) null,
	RAIL_IMP_ELIGIBLE_PV decimal(1) null,
	REG_IMP_ELIGIBLE_PV decimal(1) null,
	SAMP_PORT_GRP_PV varchar(10) null,
	SHIFT_FLAG_PV decimal(1) null,
	SHIFT_PORT_GRP_PV varchar(10) null,
	SPEND_IMP_FLAG_PV decimal(1) null,
	SPEND_IMP_ELIGIBLE_PV decimal(1) null,
	STAY_IMP_ELIGIBLE_PV decimal(1) null,
	STAY_IMP_FLAG_PV decimal(1) null,
	STAY_PURPOSE_GRP_PV decimal(2) null,
	TOWNCODE_PV varchar(10) null,
	TOWN_IMP_ELIGIBLE_PV decimal(1) null,
	TYPE_PV decimal(2) null,
	UK_OS_PV decimal(1) null,
	UKPORT1_PV decimal(4) null,
	UKPORT2_PV decimal(4) null,
	UKPORT3_PV decimal(4) null,
	UKPORT4_PV decimal(4) null,
	UNSAMP_PORT_GRP_PV varchar(10) null,
	UNSAMP_REGION_GRP_PV varchar(10) null,
	WEEKDAY_END_PV decimal(1) null,
	DIRECT decimal(6) null,
	EXPENDITURE_WT decimal(6,3) null,
	EXPENDITURE_WTK varchar(10) null,
	FAREKEY varchar(4) null,
	OVLEG decimal(6) null,
	SPEND decimal(7) null,
	SPEND1 decimal(7) null,
	SPEND2 decimal(7) null,
	SPEND3 decimal(7) null,
	SPEND4 decimal(7) null,
	SPEND5 decimal(7) null,
	SPEND6 decimal(7) null,
	SPEND7 decimal(7) null,
	SPEND8 decimal(7) null,
	SPEND9 decimal(7) null,
	SPENDIMPREASON decimal(1) null,
	SPENDK decimal(2) null,
	STAY decimal(3) null,
	STAYK decimal(1) null,
	STAY1K varchar(10) null,
	STAY2K varchar(10) null,
	STAY3K varchar(10) null,
	STAY4K varchar(10) null,
	STAY5K varchar(10) null,
	STAY6K varchar(10) null,
	STAY7K varchar(10) null,
	STAY8K varchar(10) null,
	STAY9K varchar(10) null,
	STAYTLY decimal(6) null,
	STAY_WT decimal(6,3) null,
	STAY_WTK varchar(10) null,
	TYPEINTERVIEW decimal(3) null,
	UKLEG decimal(6) null,
	VISIT_WT decimal(6,3) null,
	VISIT_WTK varchar(10) null,
	SHIFT_WT decimal(9,3) null,
	NON_RESPONSE_WT decimal(9,3) null,
	MINS_WT decimal(9,3) null,
	TRAFFIC_WT decimal(9,3) null,
	UNSAMP_TRAFFIC_WT decimal(9,3) null,
	IMBAL_WT decimal(9,3) null,
	FINAL_WT decimal(12,3) null
);

create index SURVEY_SUBSAMPLE_RUN_ID_index
	on SURVEY_SUBSAMPLE (RUN_ID);

create table SURVEY_TRAFFIC_AUX
(
	SERIAL bigint null,
	ARRIVEDEPART int null,
	PORTROUTE int null,
	SAMP_PORT_GRP_PV varchar(255) null,
	SHIFT_WT float null,
	NON_RESPONSE_WT float null,
	MINS_WT float null,
	TRAFFIC_WT varchar(5) null,
	TRAF_DESIGN_WEIGHT float null,
	T1 int null
);

create table SURVEY_UNSAMP_AUX
(
	SERIAL bigint null,
	ARRIVEDEPART int null,
	PORTROUTE int null,
	UNSAMP_PORT_GRP_PV varchar(255) null,
	UNSAMP_REGION_GRP_PV int null,
	SHIFT_WT float null,
	NON_RESPONSE_WT float null,
	MINS_WT float null,
	UNSAMP_TRAFFIC_WT varchar(5) null,
	OOH_DESIGN_WEIGHT float null,
	T1 int null
);

create table SURVEY_VALUE
(
	VERSION_ID decimal not null,
	SERIAL_NO decimal(15) not null,
	COLUMN_NO decimal(4) not null,
	COLUMN_VALUE varchar(100) not null
);

create table TASK
(
	TASK_ID varchar(40) not null,
	PARENT_ID varchar(40) null,
	SERVICE_NAME varchar(30) null,
	TASK_NAME varchar(30) not null,
	DATE_CREATED date not null,
	TASK_STATUS decimal(2) not null
);

create table TASK_CHILD
(
	TASK_ID varchar(40) not null,
	CHILD_ID varchar(40) not null
);

create table TASK_NODE
(
	TASK_ID varchar(40) not null,
	PARENT_ID varchar(40) null,
	CHILD_ID varchar(40) null,
	TASK_STATUS decimal(2) not null,
	DATE_CREATED date not null
);

create table TASK_SAS_MAP
(
	TASK_ID varchar(40) not null,
	PARAMETER_SET_ID decimal not null
);

create table TRAFFIC_DATA
(
	RUN_ID varchar(40) not null,
	YEAR decimal(4) not null,
	MONTH decimal(2) not null,
	DATA_SOURCE_ID decimal not null,
	PORTROUTE decimal(4) null,
	ARRIVEDEPART decimal(1) null,
	TRAFFICTOTAL decimal(12,3) not null,
	PERIODSTART varchar(10) null,
	PERIODEND varchar(10) null,
	AM_PM_NIGHT decimal(1) null,
	HAUL varchar(2) null,
	VEHICLE decimal(1) null
);

create table UD_SAS_OUTPUTS_VW
(
	TASK_ID varchar(160) null,
	STAT_ACT varchar(400) null,
	STAT_UNIT varchar(400) null,
	DATASET_TYPE varchar(400) null,
	PERIOD_TYPE text null,
	PERIOD_NAME varchar(960) null,
	OUTPUT varchar(960) null,
	FILE_NAME varchar(1924) null,
	VIEW_NAME varchar(120) null,
	OVERFLOW_VIEW_NAME varchar(120) null
);

create table UD_VAR_METADATA_VW
(
	VAR_NAME varchar(960) null,
	VAR_SAS_FORMAT_NAME varchar(960) null,
	VAR_VALID_FROM_DATE date null,
	VAR_DATA_TYPE varchar(424) null,
	VAR_DATA_LENGTH decimal(38) null,
	VAR_DATA_PRECISION decimal(38) null,
	VAR_DESCRIPTION text null,
	VAR_TYPE_FLAG varchar(4) null,
	VAR_LABEL varchar(960) null,
	VAR_LABEL_VALUE varchar(960) null
);

create table UNSAMPLED_OOH_DATA
(
	RUN_ID varchar(40) not null,
	YEAR decimal(4) not null,
	MONTH decimal(2) not null,
	DATA_SOURCE_ID decimal not null,
	PORTROUTE decimal(4) null,
	REGION decimal(3) null,
	ARRIVEDEPART decimal(1) null,
	UNSAMP_TOTAL decimal(12,3) not null
);

create table USER
(
	ID int auto_increment
		primary key,
	USER_NAME varchar(80) null,
	PASSWORD varchar(255) null,
	FIRST_NAME varchar(255) null,
	SURNAME varchar(255) null,
	ROLE varchar(50) null
);

create table WORKFLOW
(
	WORKFLOW_ID decimal not null,
	NAME varchar(30) null,
	PERIOD varchar(6) null
);

create table WORKFLOW_DEFINITION
(
	WORKFLOW_DEF_ID decimal(3) not null,
	WF_DEFINITION text not null
);

create table WORKSPACE_MAINTENANCE
(
	WORKSPACE_ID decimal not null,
	WORKFLOW_ID decimal not null,
	WORKSPACE varchar(60) null,
	STATE varchar(20) null,
	DATE_CREATED date null
);

create table sqlResult
(
	rownames varchar(255) null,
	RUN_ID varchar(255) null,
	YEAR int null,
	MONTH int null,
	DATA_SOURCE_ID int null,
	PORTROUTE int null,
	ARRIVEDEPART int null,
	TRAFFICTOTAL float null,
	PERIODSTART int null,
	PERIODEND int null,
	AM_PM_NIGHT int null,
	HAUL varchar(255) null,
	VEHICLE int null
);

