"""001 create initial tables

Revision ID: 66b6474a5c45
Revises: 
Create Date: 2019-07-01 17:33:53.209751

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import YEAR, LONGTEXT, DOUBLE




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
        sa.Column('PROCESS_VARIABLE_ID', sa.DECIMAL(precision=40), nullable=False),
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
        sa.Column('TRAFFIC_WT', sa.DECIMAL(DUTY_FREE_PV), nullable=True),
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
        sa.Column('TIME_STAMP', sa.DATETIME, nullable=True), #?????????????????
    )

    op.create_table(
        "RUN",
        sa.Column('RUN', sa.VARCHAR(length=40), nullable=False, primary_key=False), ###unique
        sa.Column('RUN_NAME', sa.VARCHAR(length=30), nullable=True),
        sa.Column('RUN_DESC', sa.VARCHAR(length=250), nullable=True),
        sa.Column('USER_ID', sa.VARCHAR(length=20), nullable=True),
        sa.Column('YEAR', YEAR, nullable=True),
        sa.Column('PERIOD', sa.VARCHAR(length=255), nullable=True),
        sa.Column('RUN_STATUS', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('RUN_TYPE_ID', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('LAST_MODIFIED', sa.TIMESTAMP, nullable=True),  # ?????????????????
        sa.Column('STEP', sa.VARCHAR(length=255), nullable=True),
        sa.Column('PERCENT', sa.INTEGER, nullable=True),
    )

    op.create_table(
        "RUN_STEPS",
        sa.Column('RUN', sa.VARCHAR(length=40), nullable=False),  ###unique
        sa.Column('STEP_NUMBER', sa.VARCHAR(length=2), nullable=False),
        sa.Column('STEP_NAME', sa.VARCHAR(length=50), nullable=False),
        sa.Column('STEP_STATUS', sa.VARCHAR(length=2), nullable=False),
    )

    op.create_table(
        "SAS_AIR_MILES",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),  ###unique
        sa.Column('DIRECTLEG', sa.VARCHAR(length=6), nullable=True),
        sa.Column('OVLEG', sa.VARCHAR(length=6), nullable=True),
        sa.Column('UKLEG', sa.VARCHAR(length=6), nullable=True),
    )

    op.create_table(
        "SAS_FARES_IMP",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),  ###unique
        sa.Column('FARE', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('FAREK', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('SPEND', sa.DECIMAL(precision=7), nullable=True),
        sa.Column('OPERA_PV', sa.DECIMAL(precision=1), nullable=True),
        sa.Column('SPENDIMPREASON', sa.DECIMAL(precision=1), nullable=True),

    )

    op.create_table(
        "SAS_FARES_SPV",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),  ###unique
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
        sa.Column('SERIAL', sa.DECIMAL(precisionL=15), nullable=False),
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
        sa.Column('SERIAL', sa.DECIMAL(precisionL=15), nullable=False),
        sa.Column('MINS_PORT_GRP_PV', sa.DECIMAL(precision=3), nullable=True),
        sa.Column('MINS_CTRY_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_NAT_GRP_PV', sa.DECIMAL(precision=6), nullable=True),
        sa.Column('MINS_CTRY_PORT_GRP_PV', sa.DECIMAL(precision=10), nullable=True),
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
        sa.Column('REC_ID', sa.INTEGER, nullable=False, primary_key=True),
        sa.Column('PORTROUTE', sa.DECIMAL(precision=4), nullable=True),
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
        sa.Column('MINS_CTRY_PORT_GRP_PV', sa.DECIMAL(precision=10), nullable=True),
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
        "SAS_PS_UNSAMPLED_OOH",
        sa.Column('SERIAL', sa.DECIMAL(precision=15), nullable=False),
        sa.Column('SPEND', sa.DECIMAL(precision=7), nullable=True),
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
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")
    op.drop_table("EXPORT_DATA_DOWNLOAD")

