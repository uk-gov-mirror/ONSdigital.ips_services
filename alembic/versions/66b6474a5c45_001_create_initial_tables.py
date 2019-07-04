"""001 create initial tables

Revision ID: 66b6474a5c45
Revises: 
Create Date: 2019-07-01 17:33:53.209751

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import YEAR, LONGTEXT, DOUBLE
from sqlalchemy import UniqueConstraint




# revision identifiers, used by Alembic.
revision = '66b6474a5c45'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "EXPORT_DATA_DOWNLOAD",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('DOWNLOADABLE_DATA', LONGTEXT, nullable=True), #mysql...
        sa.Column('FILENAME', sa.VARCHAR(length=40), nullable=True),
        sa.Column('SOURCE_TABLE', sa.VARCHAR(length=40), nullable=True),
        sa.Column('DATE_CREATED', sa.TEXT, nullable=True),
    )

    op.create_table(
        "NON_RESPONSE_DATA",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('YEAR', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('MONTH', sa.DECIMAL(precision=2), nullable=False),
        sa.Column('DATA_SOURCE_ID', sa.DECIMAL, nullable=False),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('WEEKDAY', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SAMPINTERVAL', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('MIGTOTAL', sa.DECIMAL, nullable=True),
        sa.Column('ORDTOTAL', sa.DECIMAL, nullable=True),
    )

    op.create_table(
        "PROCESS_VARIABLE_TESTING",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('PROCESS_VARIABLE_ID', sa.DECIMAL, nullable=False),
        sa.Column('PV_NAME', sa.VARCHAR(length=30), nullable=False),
        sa.Column('PV_DESC', sa.VARCHAR(length=1000), nullable=False),
        sa.Column('PV_DEF', sa.TEXT, nullable=False),
    )

    op.create_table(
        "PS_FINAL",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SHIFT_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('NON_RESPONSE_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MINS_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('IMBAL_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('FINAL_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "PS_IMBALANCE",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('FLOW', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('SUM_PRIOR_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('SUM_IMBAL_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
     )

    op.create_table(
        "PS_INSTRUCTION",
        sa.Column('PN_ID', sa.DECIMAL, nullable=False),
        sa.Column('PS_INSTRUCTION', sa.VARCHAR(length=2000), nullable=False),
    )

    op.create_table(
        "PS_MINIMUMS",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('MINS_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MINS_CTRY_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_NAT_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_CTRY_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('MINS_CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('FULLS_CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('PRIOR_GROSS_MINS', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('PRIOR_GROSS_FULLS', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('PRIOR_GROSS_ALL', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('MINS_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('POST_SUM', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('CASES_CARRIED_FWD', sa.DECIMAL(precision=6), nullable=True),
    )

    op.create_table(
        "PS_NON_RESPONSE",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('NR_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MEAN_RESPS_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('COUNT_RESPS', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('PRIOR_SUM', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('GROSS_RESP', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('GNR', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('MEAN_NR_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "PS_SHIFT_DATA",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('SHIFT_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MIGSI', sa.INTEGER, nullable=True),
        sa.Column('POSS_SHIFT_CROSS', sa.DECIMAL(precision=5), nullable=True),
        sa.Column('SAMP_SHIFT_CROSS', sa.DECIMAL(precision=5), nullable=True),
        sa.Column('MIN_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MEAN_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MAX_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('COUNT_RESPS', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SUM_SH_WT', sa.DECIMAL(precision=12, scale=3), nullable=True)
    )

    op.create_table(
        "PS_TRAFFIC",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('FOOT_OR_VEHICLE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TRAFFICTOTAL', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('SUM_TRAFFIC_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "PS_UNSAMPLED_OOH",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SUM_PRIOR_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('SUM_UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "RESPONSE",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('STEP_NUMBER', sa.INTEGER, nullable=False),
        sa.Column('RESPONSE_CODE', sa.INTEGER, nullable=False),
        sa.Column('MESSAGE', sa.VARCHAR(length=250), nullable=True),
        sa.Column('TIME_STAMP', sa.DATETIME, server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=True),
    )

    op.create_table(
        "RUN",
        sa.Column('RUN', sa.VARCHAR(length=40), nullable=False, primary_key=True),
        sa.Column('RUN_NAME', sa.VARCHAR(length=30), nullable=True),
        sa.Column('RUN_DESC', sa.VARCHAR(length=250), nullable=True),
        sa.Column('USER_ID', sa.VARCHAR(length=20), nullable=True),
        sa.Column('YEAR', YEAR, nullable=True),
        sa.Column('PERIOD', sa.VARCHAR(length=255), nullable=True),
        sa.Column('RUN_STATUS', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('RUN_TYPE_ID', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('LAST_MODIFIED', sa.TIMESTAMP, server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=True),  # ?????????????????
        sa.Column('STEP', sa.VARCHAR(length=255), nullable=True),
        sa.Column('PERCENT', sa.INTEGER, nullable=True),
        UniqueConstraint('RUN_ID', name='RUN_RUN_ID_uindex')
    )

    op.create_table(
        "RUN_STEPS",
        sa.Column('RUN', sa.VARCHAR(length=40), nullable=False),
        sa.Column('STEP_NUMBER', sa.DECIMAL(precision=2), nullable=False),
        sa.Column('STEP_NAME', sa.VARCHAR(length=50), nullable=False),
        sa.Column('STEP_STATUS', sa.DECIMAL(precision=2), nullable=False),
    )

    op.create_table(
        "SAS_AIR_MILES",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('DIRECTLEG', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('OVLEG', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('UKLEG', sa.DECIMAL(precision=2), nullable=True),
    )

    op.create_table(
        "SAS_FARES_IMP",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('FARE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('FAREK', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('SPEND', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('OPERA_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPENDIMPREASON', sa.DECIMAL(precision=1), nullable=True),

    )

    op.create_table(
        "SAS_FARES_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('FARES_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('FARES_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('DISCNT_PACKAGE_COST_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DISCNT_F1_PV', sa.DECIMAL(precision=4, scale=3), nullable=True),
        sa.Column('DISCNT_F2_PV', sa.DECIMAL(precision=4, scale=3), nullable=True),
        sa.Column('FAGE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('TYPE_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('UKPORT1_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('UKPORT2_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('UKPORT3_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('UKPORT4_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('OSPORT1_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('OSPORT2_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('OSPORT3_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('OSPORT4_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('APD_PV', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('QMFARE_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('DUTY_FREE_PV', sa.DECIMAL(precision=4), nullable=True),
    )

    op.create_table(
        "SAS_FINAL_WT",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('FINAL_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_IMBALANCE_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('IMBAL_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('IMBAL_PORT_SUBGRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('IMBAL_PORT_FACT_PV', sa.DECIMAL(precision=5, scale=3), nullable=True),
        sa.Column('IMBAL_CTRY_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('IMBAL_CTRY_FACT_PV', sa.DECIMAL(precision=5, scale=3), nullable=True),
        sa.Column('IMBAL_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('PURPOSE_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PURPOSE_PV', sa.DECIMAL(precision=2), nullable=True),
    )

    op.create_table(
        "SAS_IMBALANCE_WT",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('IMBAL_WT', sa.DECIMAL(precision=5, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_MINIMUMS_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('MINS_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('MINS_CTRY_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_NAT_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_CTRY_PORT_GRP_PV', sa.VARCHAR(lengrth=10), nullable=True),
        sa.Column('MINS_QUALITY_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MINS_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_MINIMUMS_WT",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('IMBAL_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_NON_RESPONSE_DATA",
        sa.Column('REC_ID', sa.INTEGER, nullable=False, primary_key=True, autoincrement=True),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('WEEKDAY', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SAMPINTERVAL', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('MIGTOTAL', sa.DECIMAL, nullable=True),
        sa.Column('ORDTOTAL', sa.DECIMAL, nullable=True),
        sa.Column('NR_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_NON_RESPONSE_PV",
        sa.Column('REC_ID', sa.DECIMAL, nullable=False),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('NR_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
    )

    op.create_table(
        "SAS_NON_RESPONSE_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('NR_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('MIG_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('NR_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_NON_RESPONSE_WT",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('NON_RESPONSE_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_PROCESS_VARIABLE",
        sa.Column('PROCVAR_NAME', sa.VARCHAR(length=30), nullable=False),
        sa.Column('PROCVAR_RULE', sa.TEXT, nullable=False),
        sa.Column('PROCVAR_ORDER', sa.DECIMAL(precision=2), nullable=False),
    )

    op.create_table(
        "SAS_PS_FINAL",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('NR_PORT_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('NON_RESPONSE_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MINS_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('IMBAL_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('FINAL_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_PS_IMBALANCE",
        sa.Column('FLOW', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('SUM_PRIOR_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('SUM_IMBAL_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_PS_MINIMUMS",
        sa.Column('MINS_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MINS_CTRY_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_NAT_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_CTRY_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('MINS_CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('FULLS_CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('PRIOR_GROSS_MINS', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('PRIOR_GROSS_FULLS', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('PRIOR_GROSS_ALL', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('MINS_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('POST_SUM', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('CASES_CARRIED_FWD', sa.DECIMAL(precision=6), nullable=True),
    )

    op.create_table(
        "SAS_PS_NON_RESPONSE",
        sa.Column('NR_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MEAN_RESPS_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('COUNT_RESPS', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('PRIOR_SUM', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('GROSS_RESP', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('GNR', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('MEAN_NR_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_PS_SHIFT_DATA",
        sa.Column('SHIFT_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MIGSI', sa.INTEGER, nullable=True),
        sa.Column('POSS_SHIFT_CROSS', sa.DECIMAL(precision=5), nullable=True),
        sa.Column('SAMP_SHIFT_CROSS', sa.DECIMAL(precision=5), nullable=True),
        sa.Column('MIN_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MEAN_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MAX_SH_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('COUNT_RESPS', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SUM_SH_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_PS_TRAFFIC",
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('FOOT_OR_VEHICLE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TRAFFICTOTAL', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('SUM_TRAFFIC_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_PS_UNSAMPLED_OOH",
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('CASES', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SUM_PRIOR_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('SUM_UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_RAIL_IMP",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SPEND', sa.DECIMAL(precision=7), nullable=True),
    )

    op.create_table(
        "SAS_RAIL_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('RAIL_CNTRY_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('RAIL_EXERCISE_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('RAIL_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_REGIONAL_IMP",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('VISIT_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('STAY_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('EXPENDITURE_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('VISIT_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('EXPENDITURE_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('NIGHTS1', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS2', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS3', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS4', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS5', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS6', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS7', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS8', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('STAY1K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY2K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY3K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY4K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY5K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY6K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY7K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY8K', sa.VARCHAR(length=10), nullable=True),
    )

    op.create_table(
        "SAS_REGIONAL_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('PURPOSE_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL4_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('REG_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_SHIFT_DATA",
        sa.Column('REC_ID', sa.INTEGER, nullable=False, autoincrement=True, primary_key=True),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('WEEKDAY', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('TOTAL', sa.DECIMAL, nullable=False),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('SHIFT_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_SHIFT_PV",
        sa.Column('REC_ID', sa.DECIMAL, nullable=False),
        sa.Column('SHIFT_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_SHIFT_SPV",
        sa.Column('REC_ID', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SHIFT_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SHIFT_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('CROSSINGS_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        UniqueConstraint('SERIAL', name='SAS_SHIFT_SPV_pk')
    )

    op.create_table(
        "SAS_SHIFT_WT",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SHIFT_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_SPEND_IMP",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False, primary_key=True),
        sa.Column('NEWSPEND', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPENDK', sa.DECIMAL(precision=2), nullable=True),
    )

    op.create_table(
        "SAS_SPEND_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SPEND_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPEND_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('UK_OS_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PUR1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('DUR1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('DUR2_PV', sa.DECIMAL(precision=8), nullable=True),
    )

    op.create_table(
        "SAS_STAY_IMP",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False, primary_key=True),
        sa.Column('STAY', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('STAYK', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_STAY_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SPEND_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPEND_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL4_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAY_PURPOSE_GRP_PV', sa.DECIMAL(precision=2), nullable=True),
    )

    op.create_table(
        "SAS_SURVEY_SUBSAMPLE",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('AGE', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('ANYUNDER16', sa.VARCHAR(length=2), nullable=True),
        sa.Column('APORTLATDEG', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLATMIN', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLATSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLATNS', sa.VARCHAR(length=1), nullable=True),
        sa.Column('APORTLONDEG', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('APORTLONMIN', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('APORTLONSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLONEW', sa.VARCHAR(length=1), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('BABYFARE', sa.DECIMAL(precision=4, scale=2), nullable=True),
        sa.Column('BEFAF', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('CHANGECODE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('CHILDFARE', sa.DECIMAL(precision=4, scale=2), nullable=True),
        sa.Column('COUNTRYVISIT', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('CPORTLATDEG', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLATMIN', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('CPORTLATSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLATNS', sa.VARCHAR(length=1), nullable=True),
        sa.Column('CPORTLONDEG', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('CPORTLONMIN', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLONSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLONEW', sa.VARCHAR(length=3), nullable=True),
        sa.Column('INTDATE', sa.VARCHAR(length=8), nullable=True),
        sa.Column('DAYTYPE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('DIRECTLEG', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVEXPEND', sa.DECIMAL(precision=6), nullable=True),

        sa.Column('DVFARE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVLINECODE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVPACKAGE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('PACKAGECOST', sa.FLOAT, nullable=True),
        sa.Column('DVPACKCOST', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVPERSONS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('DVPORTCODE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('EXPENDCODE', sa.VARCHAR(length=4), nullable=True),
        sa.Column('EXPENDITURE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('FARE', sa.DECIMAL(precision=6), nullable=True),

        sa.Column('FAREK', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('FLOW', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('HAULKEY', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('INTENDLOS', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('KIDAGE', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('LOSKEY', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('MAINCONTRA', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MIGSI', sa.INTEGER, nullable=True),
        sa.Column('INTMONTH', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('NATIONALITY', sa.DECIMAL(precision=4), nullable=True),

        sa.Column('NATIONNAME', sa.VARCHAR(length=50), nullable=True),
        sa.Column('NIGHTS1', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS2', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS3', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS4', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS5', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS6', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS7', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS8', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NUMADULTS', sa.DECIMAL(precision=3), nullable=True),

        sa.Column('NUMDAYS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NUMNIGHTS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NUMPEOPLE', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('PACKAGEHOL', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('PACKAGEHOLUK', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('PERSONS', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('PACKAGE', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELATDEG', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELATMIN', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('PROUTELATSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELATNS', sa.VARCHAR(length=1), nullable=True),
        sa.Column('PROUTELONDEG', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('PROUTELONMIN', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELONSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELONEW', sa.VARCHAR(length=1), nullable=True),
        sa.Column('PURPOSE', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('QUARTER', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('RESIDENCE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('RESPNSE', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('SEX', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SHIFTNO', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SHUTTLE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SINGLERETURN', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('TANDTSI', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TICKETCOST', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE1', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE2', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE3', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE4', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('TOWNCODE5', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE6', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE7', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE8', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TRANSFER', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('UKFOREIGN', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VEHICLE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VISITBEGAN', sa.VARCHAR(length=8), nullable=True),
        sa.Column('WELSHNIGHTS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('WELSHTOWN', sa.DECIMAL(precision=4), nullable=True),

        sa.Column('TOWNCODE5', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE6', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE7', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE8', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TRANSFER', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('UKFOREIGN', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VEHICLE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VISITBEGAN', sa.VARCHAR(length=8), nullable=True),
        sa.Column('WELSHNIGHTS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('WELSHTOWN', sa.DECIMAL(precision=4), nullable=True),

        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('APD_PV', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('ARRIVEDEPART_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('CROSSINGS_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL4_PV', sa.VARCHAR(length=8), nullable=True),
        sa.Column('DAY_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('DISCNT_F1_PV', sa.DECIMAL(precision=4, scale=3), nullable=True),

        sa.Column('DISCNT_F2_PV', sa.DECIMAL(precision=4, scale=3), nullable=True),
        sa.Column('DISCNT_PACKAGE_COST_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DUR1_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('DUR2_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('DUTY_FREE_PV', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('FAGE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('FARES_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('FARES_IMP_FLAG_PV', sa.VARCHAR(length=1), nullable=True),
        sa.Column('FLOW_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('FOOT_OR_VEHICLE_PV', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('HAUL_PV', sa.VARCHAR(length=2), nullable=True),
        sa.Column('IMBAL_CTRY_FACT_PV', sa.DECIMAL(precision=5, scale=3), nullable=True),
        sa.Column('IMBAL_CTRY_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('IMBAL_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('IMBAL_PORT_FACT_PV', sa.DECIMAL(precision=5, scale=3), nullable=True),
        sa.Column('IMBAL_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('IMBAL_PORT_SUBGRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('LOS_PV', sa.VARCHAR(length=3), nullable=True),
        sa.Column('LOSDAYS_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('MIG_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),

        sa.Column('MINS_CTRY_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_CTRY_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('MINS_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MINS_NAT_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('MINS_QUALITY_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('NR_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('NR_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('OPERA_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('OSPORT1_PV', sa.DECIMAL(precision=8), nullable=True),

        sa.Column('OSPORT2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('OSPORT3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('OSPORT4_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PURPOSE_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('QMFARE_PV', sa.VARCHAR(length=8), nullable=True),
        sa.Column('RAIL_CNTRY_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('RAIL_EXERCISE_PV', sa.DECIMAL(precision=6), nullable=True),

        sa.Column('RAIL_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('REG_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('SHIFT_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SHIFT_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('SPEND_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPEND_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAY_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAY_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAY_PURPOSE_GRP_PV', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('TOWNCODE_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('TOWN_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('TYPE_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('UK_OS_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('UKPORT1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UKPORT2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UKPORT3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UKPORT4_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),

        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('DIRECT', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('EXPENDITURE_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('EXPENDITURE_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('FAREKEY', sa.VARCHAR(length=4), nullable=True),
        sa.Column('OVLEG', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SPEND', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND1', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND2', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND3', sa.DECIMAL(precision=7), nullable=True),

        sa.Column('SPEND4', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND5', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND6', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND7', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND8', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND9', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPENDIMPREASON', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPENDK', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('STAY', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('STAYK', sa.DECIMAL(precision=1), nullable=True),

        sa.Column('STAY1K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY2K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY3K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY4K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY5K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY6K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY7K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY8K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY9K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAYTLY', sa.DECIMAL(precision=7), nullable=True),

        sa.Column('STAY_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('STAY_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('TYPEINTERVIEW', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('UKLEG', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('VISIT_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('VISIT_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('SHIFT_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('NON_RESPONSE_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MINS_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),

        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('IMBAL_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('FINAL_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
        UniqueConstraint('SERIAL', name='SAS_SURVEY_SUBSAMPLE_pk')
    )

    op.create_table(
        "SAS_TOWN_STAY_IMP",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SPEND1', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND2', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND3', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND4', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND5', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND6', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND7', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND8', sa.DECIMAL(precision=7), nullable=True),
    )

    op.create_table(
        "SAS_TOWN_STAY_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('PURPOSE_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL4_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('TOWN_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "SAS_TRAFFIC_DATA",
        sa.Column('REC_ID', sa.DECIMAL(precision=15), nullable=False, primary_key=True, autoincrement=True),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('TRAFFICTOTAL', sa.DECIMAL, nullable=True),
        sa.Column('PERIODSTART', sa.VARCHAR(length=10), nullable=True),
        sa.Column('PERIODEND', sa.VARCHAR(length=10), nullable=True),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('HAUL', sa.VARCHAR(length=2), nullable=True),
        sa.Column('VEHICLE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('FOOT_OR_VEHICLE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('HAUL_PV', sa.VARCHAR(length=2), nullable=True),
    )

    op.create_table(
        "SAS_TRAFFIC_PV",
        sa.Column('REC_ID', sa.DECIMAL, nullable=False),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('FOOT_OR_VEHICLE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('HAUL_PV', sa.VARCHAR(length=2), nullable=True),
    )

    op.create_table(
        "SAS_TRAFFIC_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('FOOT_OR_VEHICLE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('HAUL_PV', sa.VARCHAR(length=2), nullable=True),
    )

    op.create_table(
        "SAS_TRAFFIC_WT",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_UNSAMPLED_OOH_DATA",
        sa.Column('REC_ID', sa.INTEGER, nullable=False, primary_key=True, autoincrement=True),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('REGION', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('UNSAMP_TOTAL', sa.DECIMAL, nullable=True),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SAS_UNSAMPLED_OOH_PV",
        sa.Column('REC_ID', sa.DECIMAL, nullable=False),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('HAUL_PV', sa.VARCHAR(length=2), nullable=True),
    )

    op.create_table(
        "SAS_UNSAMPLED_OOH_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('HAUL_PV', sa.VARCHAR(length=2), nullable=True),
    )

    op.create_table(
        "SAS_UNSAMPLED_OOH_WT",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
    )

    op.create_table(
        "SHIFT_DATA",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('YEAR', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('MONTH', sa.DECIMAL(precision=2), nullable=False),
        sa.Column('DATA_SOURCE_ID', sa.DECIMAL, nullable=False),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('WEEKDAY', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=False),
        sa.Column('TOTAL', sa.DECIMAL, nullable=False),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=False),
    )

    op.create_table(
        "SURVEY_SUBSAMPLE",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),

        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('AGE', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('ANYUNDER16', sa.VARCHAR(length=2), nullable=True),
        sa.Column('APORTLATDEG', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLATMIN', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLATSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLATNS', sa.VARCHAR(length=1), nullable=True),
        sa.Column('APORTLONDEG', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('APORTLONMIN', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('APORTLONSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('APORTLONEW', sa.VARCHAR(length=1), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('BABYFARE', sa.DECIMAL(precision=4, scale=2), nullable=True),
        sa.Column('BEFAF', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('CHANGECODE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('CHILDFARE', sa.DECIMAL(precision=4, scale=2), nullable=True),
        sa.Column('COUNTRYVISIT', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('CPORTLATDEG', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLATMIN', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('CPORTLATSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLATNS', sa.VARCHAR(length=1), nullable=True),
        sa.Column('CPORTLONDEG', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('CPORTLONMIN', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLONSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('CPORTLONEW', sa.VARCHAR(length=3), nullable=True),
        sa.Column('INTDATE', sa.VARCHAR(length=8), nullable=True),
        sa.Column('DAYTYPE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('DIRECTLEG', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVEXPEND', sa.DECIMAL(precision=6), nullable=True),

        sa.Column('DVFARE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVLINECODE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVPACKAGE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('PACKAGECOST', sa.FLOAT, nullable=True),
        sa.Column('DVPACKCOST', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DVPERSONS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('DVPORTCODE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('EXPENDCODE', sa.VARCHAR(length=4), nullable=True),
        sa.Column('EXPENDITURE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('FARE', sa.DECIMAL(precision=6), nullable=True),

        sa.Column('FAREK', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('FLOW', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('HAULKEY', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('INTENDLOS', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('KIDAGE', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('LOSKEY', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('MAINCONTRA', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MIGSI', sa.INTEGER, nullable=True),
        sa.Column('INTMONTH', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('NATIONALITY', sa.DECIMAL(precision=4), nullable=True),

        sa.Column('NATIONNAME', sa.VARCHAR(length=50), nullable=True),
        sa.Column('NIGHTS1', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS2', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS3', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS4', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS5', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS6', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS7', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NIGHTS8', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NUMADULTS', sa.DECIMAL(precision=3), nullable=True),

        sa.Column('NUMDAYS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NUMNIGHTS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('NUMPEOPLE', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('PACKAGEHOL', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('PACKAGEHOLUK', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('PERSONS', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('PACKAGE', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELATDEG', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELATMIN', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('PROUTELATSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELATNS', sa.VARCHAR(length=1), nullable=True),
        sa.Column('PROUTELONDEG', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('PROUTELONMIN', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELONSEC', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('PROUTELONEW', sa.VARCHAR(length=1), nullable=True),
        sa.Column('PURPOSE', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('QUARTER', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('RESIDENCE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('RESPNSE', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('SEX', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SHIFTNO', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SHUTTLE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SINGLERETURN', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('TANDTSI', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TICKETCOST', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE1', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE2', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE3', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE4', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('TOWNCODE5', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE6', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE7', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE8', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TRANSFER', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('UKFOREIGN', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VEHICLE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VISITBEGAN', sa.VARCHAR(length=8), nullable=True),
        sa.Column('WELSHNIGHTS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('WELSHTOWN', sa.DECIMAL(precision=4), nullable=True),

        sa.Column('TOWNCODE5', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE6', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE7', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TOWNCODE8', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('TRANSFER', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('UKFOREIGN', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VEHICLE', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('VISITBEGAN', sa.VARCHAR(length=8), nullable=True),
        sa.Column('WELSHNIGHTS', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('WELSHTOWN', sa.DECIMAL(precision=4), nullable=True),

        sa.Column('AM_PM_NIGHT_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('APD_PV', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('ARRIVEDEPART_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('CROSSINGS_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('STAYIMPCTRYLEVEL4_PV', sa.VARCHAR(length=8), nullable=True),
        sa.Column('DAY_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('DISCNT_F1_PV', sa.DECIMAL(precision=4, scale=3), nullable=True),

        sa.Column('DISCNT_F2_PV', sa.DECIMAL(precision=4, scale=3), nullable=True),
        sa.Column('DISCNT_PACKAGE_COST_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('DUR1_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('DUR2_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('DUTY_FREE_PV', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('FAGE_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('FARES_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('FARES_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('FLOW_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('FOOT_OR_VEHICLE_PV', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('HAUL_PV', sa.VARCHAR(length=2), nullable=True),
        sa.Column('IMBAL_CTRY_FACT_PV', sa.DECIMAL(precision=5, scale=3), nullable=True),
        sa.Column('IMBAL_CTRY_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('IMBAL_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('IMBAL_PORT_FACT_PV', sa.DECIMAL(precision=5, scale=3), nullable=True),
        sa.Column('IMBAL_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('IMBAL_PORT_SUBGRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('LOS_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('LOSDAYS_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('MIG_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),

        sa.Column('MINS_CTRY_GRP_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('MINS_CTRY_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('MINS_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('MINS_NAT_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('MINS_QUALITY_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('NR_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('NR_PORT_GRP_PV', sa.VARCHAR(length=3), nullable=True),
        sa.Column('OPERA_PV', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('OSPORT1_PV', sa.DECIMAL(precision=8), nullable=True),

        sa.Column('OSPORT2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('OSPORT3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('OSPORT4_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PUR3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('PURPOSE_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('QMFARE_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('RAIL_CNTRY_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('RAIL_EXERCISE_PV', sa.DECIMAL(precision=6), nullable=True),

        sa.Column('RAIL_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('REG_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('SHIFT_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SHIFT_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('SPEND_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPEND_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAY_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAY_IMP_FLAG_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('STAY_PURPOSE_GRP_PV', sa.DECIMAL(precision=2), nullable=True),

        sa.Column('TOWNCODE_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('TOWN_IMP_ELIGIBLE_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('TYPE_PV', sa.FLOAT(precision=9, decimal_return_scale=3), nullable=True),
        sa.Column('UK_OS_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('UKPORT1_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UKPORT2_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UKPORT3_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UKPORT4_PV', sa.DECIMAL(precision=8), nullable=True),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=10), nullable=True),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),

        sa.Column('WEEKDAY_END_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('DIRECT', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('EXPENDITURE_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('EXPENDITURE_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('FAREKEY', sa.VARCHAR(length=4), nullable=True),
        sa.Column('OVLEG', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('SPEND', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND1', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND2', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND3', sa.DECIMAL(precision=7), nullable=True),

        sa.Column('SPEND4', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND5', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND6', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND7', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND8', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPEND9', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('SPENDIMPREASON', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPENDK', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('STAY', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('STAYK', sa.DECIMAL(precision=1), nullable=True),

        sa.Column('STAY1K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY2K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY3K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY4K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY5K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY6K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY7K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY8K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAY9K', sa.VARCHAR(length=10), nullable=True),
        sa.Column('STAYTLY', sa.DECIMAL(precision=7), nullable=True),

        sa.Column('STAY_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('STAY_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('TYPEINTERVIEW', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('UKLEG', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('VISIT_WT', sa.DECIMAL(precision=6, scale=3), nullable=True),
        sa.Column('VISIT_WTK', sa.VARCHAR(length=10), nullable=True),
        sa.Column('SHIFT_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('NON_RESPONSE_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('MINS_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),

        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('IMBAL_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('FINAL_WT', sa.DECIMAL(precision=12, scale=3), nullable=True),
    )

    op.create_table(
        "TRAFFIC_DATA",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('YEAR', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('MONTH', sa.DECIMAL(precision=2), nullable=False),
        sa.Column('DATA_SOURCE_ID', sa.DECIMAL, nullable=False),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('TRAFFICTOTAL', sa.DECIMAL(precision=12, scale=3), nullable=False),
        sa.Column('PERIODSTART', sa.VARCHAR(length=10), nullable=True),
        sa.Column('PERIODEND', sa.VARCHAR(length=10), nullable=True),
        sa.Column('AM_PM_NIGHT', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('HAUL', sa.VARCHAR(length=2), nullable=True),
        sa.Column('VEHICLE', sa.DECIMAL(precision=1), nullable=True),
    )

    op.create_table(
        "UNSAMPLED_OOH_DATA",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('YEAR', sa.DECIMAL(precision=4), nullable=False),
        sa.Column('MONTH', sa.DECIMAL(precision=2), nullable=False),
        sa.Column('DATA_SOURCE_ID', sa.DECIMAL, nullable=False),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=True),
        sa.Column('REGION', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('ARRIVEDEPART', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('UNSAMP_TOTAL', sa.VARCHAR(length=12, scale=3), nullable=False),
    )

    op.create_table(
        "POPROWVEC_TRAFFIC",
        sa.Column('`index`', sa.BIGINT, nullable=True),
        sa.Column('C_GROUP', sa.BIGINT, nullable=True),
        sa.Column('T_1', DOUBLE, nullable=True),
        sa.Column('T_2', DOUBLE, nullable=True),
        sa.Column('T_3', DOUBLE, nullable=True),
        sa.Column('T_4', DOUBLE, nullable=True),
        sa.Column('T_5', DOUBLE, nullable=True),
        sa.Column('T_6', DOUBLE, nullable=True),
        sa.Column('T_7', DOUBLE, nullable=True),
        sa.Column('T_8', DOUBLE, nullable=True),
        sa.Column('T_9', DOUBLE, nullable=True),
        sa.Column('T_10', DOUBLE, nullable=True),
        sa.Column('T_11', DOUBLE, nullable=True),
        sa.Column('T_12', DOUBLE, nullable=True),
        sa.Column('T_13', DOUBLE, nullable=True),
        sa.Column('T_14', DOUBLE, nullable=True),
        sa.Column('T_15', DOUBLE, nullable=True),
        sa.Column('T_16', DOUBLE, nullable=True),
        sa.Column('T_17', DOUBLE, nullable=True),
        sa.Column('T_18', DOUBLE, nullable=True),
        sa.Column('T_19', DOUBLE, nullable=True),
        sa.Column('T_20', DOUBLE, nullable=True),
        sa.Column('T_21', DOUBLE, nullable=True),
        sa.Column('T_22', DOUBLE, nullable=True),
        sa.Column('T_23', DOUBLE, nullable=True),
        sa.Column('T_24', DOUBLE, nullable=True),
        sa.Column('T_25', DOUBLE, nullable=True),
        sa.Column('T_26', DOUBLE, nullable=True),
        sa.Column('T_27', DOUBLE, nullable=True),
        sa.Column('T_28', DOUBLE, nullable=True),
        sa.Column('T_29', DOUBLE, nullable=True),
        sa.Column('T_30', DOUBLE, nullable=True),
        sa.Column('T_31', DOUBLE, nullable=True),
        sa.Column('T_32', DOUBLE, nullable=True),
        sa.Column('T_33', DOUBLE, nullable=True),
        sa.Column('T_34', DOUBLE, nullable=True),
        sa.Column('T_35', DOUBLE, nullable=True),
        sa.Column('T_36', DOUBLE, nullable=True),
        sa.Column('T_37', DOUBLE, nullable=True),
        sa.Column('T_38', DOUBLE, nullable=True),
        sa.Column('T_39', DOUBLE, nullable=True),
        sa.Column('T_40', DOUBLE, nullable=True),
        sa.Column('T_41', DOUBLE, nullable=True),
        sa.Column('T_42', DOUBLE, nullable=True),
        sa.Column('T_43', DOUBLE, nullable=True),
        sa.Column('T_44', DOUBLE, nullable=True),
        sa.Column('T_45', DOUBLE, nullable=True),
        sa.Column('T_46', DOUBLE, nullable=True),
        sa.Column('T_47', DOUBLE, nullable=True),
        sa.Column('T_48', DOUBLE, nullable=True),
        sa.Column('T_49', DOUBLE, nullable=True),
        sa.Column('T_50', DOUBLE, nullable=True),
        sa.Column('T_51', DOUBLE, nullable=True),
        sa.Column('T_52', DOUBLE, nullable=True),
        sa.Column('T_53', DOUBLE, nullable=True),
        sa.Column('T_54', DOUBLE, nullable=True),
        sa.Column('T_55', DOUBLE, nullable=True),
        sa.Column('T_56', DOUBLE, nullable=True),
        sa.Column('T_57', DOUBLE, nullable=True),
        sa.Column('T_58', DOUBLE, nullable=True),
        sa.Column('T_59', DOUBLE, nullable=True),
        sa.Column('T_60', DOUBLE, nullable=True),
        sa.Column('T_61', DOUBLE, nullable=True),
        sa.Column('T_62', DOUBLE, nullable=True),
        sa.Column('T_63', DOUBLE, nullable=True),
        sa.Column('T_64', DOUBLE, nullable=True),
        sa.Column('T_65', DOUBLE, nullable=True),
        sa.Column('T_66', DOUBLE, nullable=True),
        sa.Column('T_67', DOUBLE, nullable=True),
        sa.Column('T_68', DOUBLE, nullable=True),
        sa.Column('T_69', DOUBLE, nullable=True),
        sa.Column('T_70', DOUBLE, nullable=True),
        sa.Column('T_71', DOUBLE, nullable=True),
        sa.Column('T_72', DOUBLE, nullable=True),
        sa.Column('T_73', DOUBLE, nullable=True),
        sa.Column('T_74', DOUBLE, nullable=True),
        sa.Column('T_75', DOUBLE, nullable=True),
        sa.Column('T_76', DOUBLE, nullable=True),
        sa.Column('T_77', DOUBLE, nullable=True),
    )

    op.create_table(
        "POPROWVEC_TRAFFIC",
        sa.Column('C_GROUP', sa.BIGINT, nullable=True),
        sa.Column('T_1', sa.FLOAT, nullable=True),
        sa.Column('T_2', sa.FLOAT, nullable=True),
        sa.Column('T_3', sa.FLOAT, nullable=True),
        sa.Column('T_4', sa.FLOAT, nullable=True),
        sa.Column('T_5', sa.FLOAT, nullable=True),
        sa.Column('T_6', sa.FLOAT, nullable=True),
        sa.Column('T_7', sa.FLOAT, nullable=True),
        sa.Column('T_8', sa.FLOAT, nullable=True),
        sa.Column('T_9', sa.FLOAT, nullable=True),
        sa.Column('T_10', sa.FLOAT, nullable=True),
        sa.Column('T_11', sa.FLOAT, nullable=True),
        sa.Column('T_12', sa.FLOAT, nullable=True),
        sa.Column('T_13', sa.FLOAT, nullable=True),
        sa.Column('T_14', sa.FLOAT, nullable=True),
        sa.Column('T_15', sa.FLOAT, nullable=True),
        sa.Column('T_16', sa.FLOAT, nullable=True),
        sa.Column('T_17', sa.FLOAT, nullable=True),
        sa.Column('T_18', sa.FLOAT, nullable=True),
        sa.Column('T_19', sa.FLOAT, nullable=True),
        sa.Column('T_20', sa.FLOAT, nullable=True),
        sa.Column('T_21', sa.FLOAT, nullable=True),
        sa.Column('T_22', sa.FLOAT, nullable=True),
        sa.Column('T_23', sa.FLOAT, nullable=True),
        sa.Column('T_24', sa.FLOAT, nullable=True),
        sa.Column('T_25', sa.FLOAT, nullable=True),
        sa.Column('T_26', sa.FLOAT, nullable=True),
        sa.Column('T_27', sa.FLOAT, nullable=True),
        sa.Column('T_28', sa.FLOAT, nullable=True),
        sa.Column('T_29', sa.FLOAT, nullable=True),
        sa.Column('T_30', sa.FLOAT, nullable=True),
        sa.Column('T_31', sa.FLOAT, nullable=True),
        sa.Column('T_32', sa.FLOAT, nullable=True),
        sa.Column('T_33', sa.FLOAT, nullable=True),
        sa.Column('T_34', sa.FLOAT, nullable=True),
        sa.Column('T_35', sa.FLOAT, nullable=True),
        sa.Column('T_36', sa.FLOAT, nullable=True),
        sa.Column('T_37', sa.FLOAT, nullable=True),
        sa.Column('T_38', sa.FLOAT, nullable=True),
        sa.Column('T_39', sa.FLOAT, nullable=True),
        sa.Column('T_30', sa.FLOAT, nullable=True),
        sa.Column('T_41', sa.FLOAT, nullable=True),
        sa.Column('T_42', sa.FLOAT, nullable=True),
        sa.Column('T_43', sa.FLOAT, nullable=True),
        sa.Column('T_44', sa.FLOAT, nullable=True),
        sa.Column('T_44', sa.FLOAT, nullable=True),
        sa.Column('T_44', sa.FLOAT, nullable=True),
        sa.Column('T_47', sa.FLOAT, nullable=True),
        sa.Column('T_48', sa.FLOAT, nullable=True),
        sa.Column('T_49', sa.FLOAT, nullable=True),
        sa.Column('T_50', sa.FLOAT, nullable=True),
        sa.Column('T_51', sa.FLOAT, nullable=True),
        sa.Column('T_52', sa.FLOAT, nullable=True),
        sa.Column('T_53', sa.FLOAT, nullable=True),
        sa.Column('T_54', sa.FLOAT, nullable=True),
        sa.Column('T_55', sa.FLOAT, nullable=True),
        sa.Column('T_56', sa.FLOAT, nullable=True),
        sa.Column('T_57', sa.FLOAT, nullable=True),
        sa.Column('T_58', sa.FLOAT, nullable=True),
        sa.Column('T_59', sa.FLOAT, nullable=True),
        sa.Column('T_60', sa.FLOAT, nullable=True),
        sa.Column('T_61', sa.FLOAT, nullable=True),
        sa.Column('T_62', sa.FLOAT, nullable=True),
        sa.Column('T_63', sa.FLOAT, nullable=True),
        sa.Column('T_64', sa.FLOAT, nullable=True),
        sa.Column('T_65', sa.FLOAT, nullable=True),
        sa.Column('T_66', sa.FLOAT, nullable=True),
        sa.Column('T_67', sa.FLOAT, nullable=True),
        sa.Column('T_68', sa.FLOAT, nullable=True),
        sa.Column('T_69', sa.FLOAT, nullable=True),
        sa.Column('T_70', sa.FLOAT, nullable=True),
        sa.Column('T_71', sa.FLOAT, nullable=True),
        sa.Column('T_72', sa.FLOAT, nullable=True),
        sa.Column('T_73', sa.FLOAT, nullable=True),
        sa.Column('T_74', sa.FLOAT, nullable=True),
        sa.Column('T_75', sa.FLOAT, nullable=True),
        sa.Column('T_76', sa.FLOAT, nullable=True),
        sa.Column('T_77', sa.FLOAT, nullable=True),
        sa.Column('T_78', sa.FLOAT, nullable=True),
        sa.Column('T_79', sa.FLOAT, nullable=True),
        sa.Column('T_80', sa.FLOAT, nullable=True),
        sa.Column('T_81', sa.FLOAT, nullable=True),
        sa.Column('T_82', sa.FLOAT, nullable=True),
        sa.Column('T_83', sa.FLOAT, nullable=True),
        sa.Column('T_84', sa.FLOAT, nullable=True),
        sa.Column('T_85', sa.FLOAT, nullable=True),
        sa.Column('T_86', sa.FLOAT, nullable=True),
        sa.Column('T_87', sa.FLOAT, nullable=True),
        sa.Column('T_88', sa.FLOAT, nullable=True),
        sa.Column('T_89', sa.FLOAT, nullable=True),
        sa.Column('T_90', sa.FLOAT, nullable=True),
        sa.Column('T_91', sa.FLOAT, nullable=True),
        sa.Column('T_92', sa.FLOAT, nullable=True),
        sa.Column('T_93', sa.FLOAT, nullable=True),
        sa.Column('T_94', sa.FLOAT, nullable=True),
        sa.Column('T_95', sa.FLOAT, nullable=True),
        sa.Column('T_96', sa.FLOAT, nullable=True),
        sa.Column('T_97', sa.FLOAT, nullable=True),
        sa.Column('T_98', sa.FLOAT, nullable=True),
        sa.Column('T_99', sa.FLOAT, nullable=True),
        sa.Column('T_100', sa.FLOAT, nullable=True),
        sa.Column('T_101', sa.FLOAT, nullable=True),
        sa.Column('T_102', sa.FLOAT, nullable=True),
        sa.Column('T_103', sa.FLOAT, nullable=True),
        sa.Column('T_104', sa.FLOAT, nullable=True),
        sa.Column('T_105', sa.FLOAT, nullable=True),
        sa.Column('T_106', sa.FLOAT, nullable=True),
        sa.Column('T_107', sa.FLOAT, nullable=True),
        sa.Column('T_108', sa.FLOAT, nullable=True),
        sa.Column('T_109', sa.FLOAT, nullable=True),
        sa.Column('T_110', sa.FLOAT, nullable=True),
        sa.Column('T_111', sa.FLOAT, nullable=True),
        sa.Column('T_112', sa.FLOAT, nullable=True),
        sa.Column('T_113', sa.FLOAT, nullable=True),
        sa.Column('T_114', sa.FLOAT, nullable=True),
        sa.Column('T_115', sa.FLOAT, nullable=True),
        sa.Column('T_116', sa.FLOAT, nullable=True),
        sa.Column('T_117', sa.FLOAT, nullable=True),
        sa.Column('T_118', sa.FLOAT, nullable=True),
        sa.Column('T_119', sa.FLOAT, nullable=True),
        sa.Column('T_120', sa.FLOAT, nullable=True),
        sa.Column('T_121', sa.FLOAT, nullable=True),
        sa.Column('T_122', sa.FLOAT, nullable=True),
        sa.Column('T_123', sa.FLOAT, nullable=True),
        sa.Column('T_124', sa.FLOAT, nullable=True),
        sa.Column('T_125', sa.FLOAT, nullable=True),
        sa.Column('T_126', sa.FLOAT, nullable=True),
        sa.Column('T_127', sa.FLOAT, nullable=True),
        sa.Column('T_128', sa.FLOAT, nullable=True),
        sa.Column('T_129', sa.FLOAT, nullable=True),
        sa.Column('T_130', sa.FLOAT, nullable=True),
        sa.Column('T_131', sa.FLOAT, nullable=True),
        sa.Column('T_132', sa.FLOAT, nullable=True),
        sa.Column('T_133', sa.FLOAT, nullable=True),
        sa.Column('T_134', sa.FLOAT, nullable=True),
        sa.Column('T_135', sa.FLOAT, nullable=True),
        sa.Column('T_136', sa.FLOAT, nullable=True),
        sa.Column('T_137', sa.FLOAT, nullable=True),
        sa.Column('T_138', sa.FLOAT, nullable=True),
        sa.Column('T_139', sa.FLOAT, nullable=True),
        sa.Column('T_130', sa.FLOAT, nullable=True),
        sa.Column('T_141', sa.FLOAT, nullable=True),
        sa.Column('T_142', sa.FLOAT, nullable=True),
        sa.Column('T_143', sa.FLOAT, nullable=True),
        sa.Column('T_144', sa.FLOAT, nullable=True),
        sa.Column('T_144', sa.FLOAT, nullable=True),
        sa.Column('T_144', sa.FLOAT, nullable=True),
        sa.Column('T_147', sa.FLOAT, nullable=True),
        sa.Column('T_148', sa.FLOAT, nullable=True),
        sa.Column('T_149', sa.FLOAT, nullable=True),
        sa.Column('T_150', sa.FLOAT, nullable=True),
        sa.Column('T_151', sa.FLOAT, nullable=True),
        sa.Column('T_152', sa.FLOAT, nullable=True),
        sa.Column('T_153', sa.FLOAT, nullable=True),
        sa.Column('T_154', sa.FLOAT, nullable=True),
        sa.Column('T_155', sa.FLOAT, nullable=True),
        sa.Column('T_156', sa.FLOAT, nullable=True),
        sa.Column('T_157', sa.FLOAT, nullable=True),
        sa.Column('T_158', sa.FLOAT, nullable=True),
        sa.Column('T_159', sa.FLOAT, nullable=True),
        sa.Column('T_160', sa.FLOAT, nullable=True),
        sa.Column('T_161', sa.FLOAT, nullable=True),
        sa.Column('T_162', sa.FLOAT, nullable=True),
        sa.Column('T_163', sa.FLOAT, nullable=True),
        sa.Column('T_164', sa.FLOAT, nullable=True),
        sa.Column('T_165', sa.FLOAT, nullable=True),
        sa.Column('T_166', sa.FLOAT, nullable=True),
        sa.Column('T_167', sa.FLOAT, nullable=True),
        sa.Column('T_168', sa.FLOAT, nullable=True),
        sa.Column('T_169', sa.FLOAT, nullable=True),
        sa.Column('T_170', sa.FLOAT, nullable=True),
        sa.Column('T_171', sa.FLOAT, nullable=True),
        sa.Column('T_172', sa.FLOAT, nullable=True),
        sa.Column('T_173', sa.FLOAT, nullable=True),
        sa.Column('T_174', sa.FLOAT, nullable=True),
        sa.Column('T_175', sa.FLOAT, nullable=True),
        sa.Column('T_176', sa.FLOAT, nullable=True),
        sa.Column('T_177', sa.FLOAT, nullable=True),
        sa.Column('T_178', sa.FLOAT, nullable=True),
        sa.Column('T_179', sa.FLOAT, nullable=True),
        sa.Column('T_180', sa.FLOAT, nullable=True),
        sa.Column('T_181', sa.FLOAT, nullable=True),
        sa.Column('T_182', sa.FLOAT, nullable=True),
        sa.Column('T_183', sa.FLOAT, nullable=True),
        sa.Column('T_184', sa.FLOAT, nullable=True),
        sa.Column('T_185', sa.FLOAT, nullable=True),
        sa.Column('T_186', sa.FLOAT, nullable=True),
        sa.Column('T_187', sa.FLOAT, nullable=True),
        sa.Column('T_188', sa.FLOAT, nullable=True),
        sa.Column('T_189', sa.FLOAT, nullable=True),
        sa.Column('T_190', sa.FLOAT, nullable=True),
        sa.Column('T_191', sa.FLOAT, nullable=True),
        sa.Column('T_192', sa.FLOAT, nullable=True),
        sa.Column('T_193', sa.FLOAT, nullable=True),
        sa.Column('T_194', sa.FLOAT, nullable=True),
        sa.Column('T_195', sa.FLOAT, nullable=True),
        sa.Column('T_196', sa.FLOAT, nullable=True),
        sa.Column('T_197', sa.FLOAT, nullable=True),
        sa.Column('T_198', sa.FLOAT, nullable=True),
        sa.Column('T_199', sa.FLOAT, nullable=True),
        sa.Column('T_200', sa.FLOAT, nullable=True),
        sa.Column('T_201', sa.FLOAT, nullable=True),
        sa.Column('T_202', sa.FLOAT, nullable=True),
        sa.Column('T_203', sa.FLOAT, nullable=True),
    )

    op.create_table(
        "R_TRAFFIC",
        sa.Column('rownames', sa.VARCHAR(length=255), nullable=True),
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=True),
        sa.Column('ARRIVEDEPART', sa.INTEGER, nullable=True),
        sa.Column('PORTROUTE', sa.INTEGER, nullable=True),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=255), nullable=True),
        sa.Column('SHIFT_WT', sa.FLOAT, nullable=True),
        sa.Column('NON_RESPONSE_WT', DOUBLE, nullable=True),
        sa.Column('MINS_WT', DOUBLE, nullable=True),
        sa.Column('TRAFFIC_WT', DOUBLE, nullable=True),
        sa.Column('TRAF_DESIGN_WEIGHT', DOUBLE, nullable=True),
        sa.Column('T1', sa.INTEGER, nullable=True),
        sa.Column('T_', sa.VARCHAR(length=255), nullable=True),
        sa.Column('TW_WEIGHT', DOUBLE, nullable=True),
    )

    op.create_table(
        "R_UNSAMPLED",
        sa.Column('rownames', sa.VARCHAR(length=255), nullable=True),
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=True),
        sa.Column('ARRIVEDEPART', sa.INTEGER, nullable=True),
        sa.Column('PORTROUTE', sa.INTEGER, nullable=True),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=255), nullable=True),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('SHIFT_WT', sa.FLOAT, nullable=True),
        sa.Column('NON_RESPONSE_WT', sa.FLOAT, nullable=True),
        sa.Column('MINS_WT', sa.FLOAT, nullable=True),
        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('OOH_DESIGN_WEIGHT', sa.FLOAT, nullable=True),
        sa.Column('T1', sa.INTEGER, nullable=True),
        sa.Column('T_', sa.VARCHAR(length=255), nullable=True),
        sa.Column('UNSAMP_TRAFFIC_WEIGHT', sa.FLOAT, nullable=True),
    )

    op.create_table(
        "SURVEY_TRAFFIC_AUX",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=True),
        sa.Column('ARRIVEDEPART', sa.INTEGER, nullable=True),
        sa.Column('PORTROUTE', sa.INTEGER, nullable=True),
        sa.Column('SAMP_PORT_GRP_PV', sa.VARCHAR(length=255), nullable=True),
        sa.Column('SHIFT_WT', sa.FLOAT, nullable=True),
        sa.Column('NON_RESPONSE_WT', sa.FLOAT, nullable=True),
        sa.Column('MINS_WT', sa.FLOAT, nullable=True),
        sa.Column('TRAFFIC_WT', sa.VARCHAR(length=5), nullable=True),
        sa.Column('TRAF_DESIGN_WEIGHT', sa.FLOAT, nullable=True),
        sa.Column('T1', sa.INTEGER, nullable=True),
    )

    op.create_table(
        "SURVEY_UNSAMP_AUX",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=True),
        sa.Column('ARRIVEDEPART', sa.INTEGER, nullable=True),
        sa.Column('PORTROUTE', sa.INTEGER, nullable=True),
        sa.Column('UNSAMP_PORT_GRP_PV', sa.VARCHAR(length=255), nullable=True),
        sa.Column('UNSAMP_REGION_GRP_PV', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('SHIFT_WT', sa.FLOAT, nullable=True),
        sa.Column('NON_RESPONSE_WT', sa.FLOAT, nullable=True),
        sa.Column('MINS_WT', sa.FLOAT, nullable=True),
        sa.Column('UNSAMP_TRAFFIC_WT', sa.DECIMAL(precision=9, scale=3), nullable=True),
        sa.Column('OOH_DESIGN_WEIGHT', sa.FLOAT, nullable=True),
        sa.Column('T1', sa.INTEGER, nullable=True),
    )


def downgrade():
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("NON_RESPONSE_DATA")
    op.drop_table("PROCESS_VARIABLE_TESTING")
    op.drop_table("PS_FINAL")
    op.drop_table("PS_IMBALANCE")
    op.drop_table("PS_INSTRUCTION")
    op.drop_table("PS_MINIMUMS")
    op.drop_table("PS_NON_RESPONSE")
    op.drop_table("PS_SHIFT_DATA")
    op.drop_table("PS_TRAFFIC")
    op.drop_table("PS_UNSAMPLED_OOH")
    op.drop_table("RESPONSE")
    op.drop_table("RUN")
    op.drop_table("RUN_STEPS")
    op.drop_table("SAS_AIR_MILES")
    op.drop_table("SAS_FARES_IMP")
    op.drop_table("SAS_FARES_SPV")
    op.drop_table("SAS_FINAL_WT")
    op.drop_table("SAS_IMBALANCE_SPV")
    op.drop_table("SAS_IMBALANCE_WT")
    op.drop_table("SAS_MINIMUMS_SPV")
    op.drop_table("SAS_MINIMUMS_WT")
    op.drop_table("SAS_NON_RESPONSE_DATA")
    op.drop_table("SAS_NON_RESPONSE_PV")
    op.drop_table("SAS_NON_RESPONSE_SPV")
    op.drop_table("SAS_NON_RESPONSE_WT")
    op.drop_table("SAS_PROCESS_VARIABLE")
    op.drop_table("SAS_PS_FINAL")
    op.drop_table("SAS_PS_IMBALANCE")
    op.drop_table("SAS_PS_MINIMUMS")
    op.drop_table("SAS_PS_NON_RESPONSE")
    op.drop_table("SAS_PS_SHIFT_DATA")
    op.drop_table("SAS_PS_TRAFFIC")
    op.drop_table("SAS_PS_UNSAMPLED_OOH")
    op.drop_table("SAS_RAIL_IMP")
    op.drop_table("SAS_RAIL_SPV")
    op.drop_table("SAS_REGIONAL_IMP")
    op.drop_table("SAS_REGIONAL_SPV")
    op.drop_table("SAS_SHIFT_DATA")
    op.drop_table("SAS_SHIFT_PV")
    op.drop_table("SAS_SHIFT_SPV")
    op.drop_table("SAS_SHIFT_WT")
    op.drop_table("SAS_SPEND_SPV")
    op.drop_table("SAS_STAY_IMP")
    op.drop_table("SAS_STAY_SPV")
    op.drop_table("SAS_SURVEY_SUBSAMPLE")
    op.drop_table("SAS_TOWN_STAY_IMP")
    op.drop_table("SAS_TOWN_STAY_SPV")
    op.drop_table("SAS_TRAFFIC_DATA")
    op.drop_table("SAS_TRAFFIC_PV")
    op.drop_table("SAS_TRAFFIC_SPV")
    op.drop_table("SAS_TRAFFIC_WT")
    op.drop_table("SAS_UNSAMPLED_OOH_DATA")
    op.drop_table("SAS_UNSAMPLED_OOH_PV")
    op.drop_table("SAS_UNSAMPLED_OOH_SPV")
    op.drop_table("SAS_UNSAMPLED_OOH_WT")
    op.drop_table("SHIFT_DATA")
    op.drop_table("SURVEY_SUBSAMPLE")
    op.drop_table("TRAFFIC_DATA")
    op.drop_table("UNSAMPLED_OOH_DATA")
    op.drop_table("POPROWVEC_TRAFFIC")
    op.drop_table("POPROWVEC_UNSAMP")
    op.drop_table("R_TRAFFIC")
    op.drop_table("R_UNSAMPLED")
    op.drop_table("SURVEY_TRAFFIC_AUX")
    op.drop_table("SURVEY_UNSAMP_AUX")

