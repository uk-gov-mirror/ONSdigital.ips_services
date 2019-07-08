"""002 insert data and indexes

Revision ID: 7fe61c4343bb
Revises: 66b6474a5c45
Create Date: 2019-07-04 13:08:42.388831

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import YEAR
from sqlalchemy import *
from db.data.PVs import *

# revision identifiers, used by Alembic.
revision = '7fe61c4343bb'
down_revision = '66b6474a5c45'
branch_labels = None
depends_on = None


def upgrade():
    PROCESS_VARIABLE_PY = op.create_table(
        "PROCESS_VARIABLE_PY",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('PROCESS_VARIABLE_ID', sa.DECIMAL, nullable=False),
        sa.Column('PV_NAME', sa.VARCHAR(length=30), nullable=False),
        sa.Column('PV_DESC', sa.VARCHAR(length=1000), nullable=False),
        sa.Column('PV_DEF', sa.TEXT, nullable=False),
    )

    PROCESS_VARIABLE_SET = op.create_table(
        "PROCESS_VARIABLE_SET",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False),
        sa.Column('NAME', sa.VARCHAR(length=30), nullable=False),
        sa.Column('USER', sa.VARCHAR(length=50), nullable=False),
        sa.Column('PERIOD', sa.VARCHAR(length=12), nullable=False),
        sa.Column('YEAR', YEAR(display_width=4), nullable=False),
    )

    RUN = op.create_table(
        "RUN",
        sa.Column('RUN_ID', sa.VARCHAR(length=40), nullable=False, primary_key=True),
        sa.Column('RUN_NAME', sa.VARCHAR(length=30), nullable=True),
        sa.Column('RUN_DESC', sa.VARCHAR(length=250), nullable=True),
        sa.Column('USER_ID', sa.VARCHAR(length=20), nullable=True),
        sa.Column('YEAR', YEAR, nullable=True),
        sa.Column('PERIOD', sa.VARCHAR(length=255), nullable=True),
        sa.Column('RUN_STATUS', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('RUN_TYPE_ID', sa.DECIMAL(precision=2), nullable=True),
        sa.Column('LAST_MODIFIED', sa.TIMESTAMP, server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('STEP', sa.VARCHAR(length=255), nullable=True),
        sa.Column('PERCENT', sa.INTEGER, nullable=True),
        UniqueConstraint('RUN_ID', name='RUN_RUN_ID_uindex')
    )

    op.bulk_insert(RUN,
                   [
                       {"RUN_ID": "b63786be-25b1-4f30-bfd9-a240a10f0ede", "RUN_NAME": "TM_Create_and_Edit_Test", "RUN_DESC": "Test run to check run creation and editing are still working after changing the fieldwork selection options", "USER_ID": "smptester", "PERIOD": "01", "YEAR": 2019, "RUN_STATUS": 0, "RUN_TYPE_ID": "0", "LAST_MODIFIED": "2019-07-05"},
                       {"RUN_ID": "0eada784-7caf-4f68-b26f-4699c9bf0032", "RUN_NAME": "TestRun1", "RUN_DESC": "Demo test run", "USER_ID": "smptester", "PERIOD": "02", "YEAR": 2018, "RUN_STATUS": 3, "RUN_TYPE_ID": 0, "LAST_MODIFIED": "2019-07-05"},
                       {"RUN_ID": "09e5c1872-3f8e-4ae5-85dc-c67a602d011e", "RUN_NAME": "IPS_Test_Run_December_2017", "RUN_DESC": "IPS run that contains data for the December period of 2017. This is our demo run, skip the dataimport step.", "USER_ID": "smptester", "PERIOD": "12", "YEAR": 2017, "RUN_STATUS": 2, "RUN_TYPE_ID": 1, "LAST_MODIFIED": "2019-07-05"},
                       {"RUN_ID": "6aee5893-79bd-4e6b-923e-c41a9d3b56d9", "RUN_NAME": "IPS_Run December 2017", "RUN_DESC": "Demo Run", "USER_ID": "smptester", "PERIOD": "11", "YEAR": 2017, "RUN_STATUS": 3, "RUN_TYPE_ID": "0", "LAST_MODIFIED": "2019-07-05"},
                       {"RUN_ID": "b33e6aa9-415a-408f-a871-04701fadbd70", "RUN_NAME": "HandoverRun", "RUN_DESC": "Test run for handover demo.", "USER_ID": "mahont1", "PERIOD": "01", "YEAR": 2019, "RUN_STATUS": 0, "RUN_TYPE_ID": 0, "LAST_MODIFIED": "2019-07-05"},
                   ])

    op.bulk_insert(PROCESS_VARIABLE_SET,
                   [
                       {"RUN_ID": "TEMPLATE", "NAME": "TEMPLATE", "USER": "Template", "PERIOD": "2017", "YEAR": 2017}
                   ])

    op.bulk_insert(PROCESS_VARIABLE_PY,
                   [
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 12, 'PV_NAME': 'imbal_port_fact_pv', 'PV_DESC': 'imbal_port_fact_pv', 'PV_DEF': imbal_port_fact_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 13, 'PV_NAME': 'stay_imp_flag_pv', 'PV_DESC': 'stay_imp_flag_pv', 'PV_DEF': stay_imp_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 14, 'PV_NAME': 'stay_imp_eligible_pv', 'PV_DESC': 'stay_imp_eligible_pv', 'PV_DEF': stay_imp_eligible_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 33, 'PV_NAME': 'osport2_pv', 'PV_DESC': 'osport2_pv', 'PV_DEF': osport2_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 47, 'PV_NAME': 'rail_cntry_grp_pv', 'PV_DESC': 'rail_cntry_grp_pv', 'PV_DEF': rail_cntry_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 34, 'PV_NAME': 'osport3_pv', 'PV_DESC': 'osport3_pv', 'PV_DEF': osport3_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 35, 'PV_NAME': 'osport4_pv', 'PV_DESC': 'osport4_pv', 'PV_DEF': osport4_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 36, 'PV_NAME': 'apd_pv', 'PV_DESC': 'apd_pv', 'PV_DEF': apd_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 37, 'PV_NAME': 'qmfare_pv', 'PV_DESC': 'qmfare_pv', 'PV_DEF': qmfare_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 38, 'PV_NAME': 'duty_free_pv', 'PV_DESC': 'duty_free_pv', 'PV_DEF': duty_free_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 39, 'PV_NAME': 'spend_imp_eligible_pv', 'PV_DESC': 'spend_imp_eligible_pv', 'PV_DEF': spend_imp_eligible_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 40, 'PV_NAME': 'uk_os_pv', 'PV_DESC': 'uk_os_pv', 'PV_DEF': uk_os_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 41, 'PV_NAME': 'pur1_pv', 'PV_DESC': 'pur1_pv', 'PV_DEF': pur1_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 42, 'PV_NAME': 'pur2_pv', 'PV_DESC': 'pur2_pv', 'PV_DEF': pur2_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 43, 'PV_NAME': 'pur3_pv', 'PV_DESC': 'pur3_pv', 'PV_DEF': pur3_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 44, 'PV_NAME': 'dur1_pv', 'PV_DESC': 'dur1_pv', 'PV_DEF': dur1_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 45, 'PV_NAME': 'dur2_pv', 'PV_DESC': 'dur2_pv', 'PV_DEF': dur2_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 46, 'PV_NAME': 'imbal_ctry_fact_pv', 'PV_DESC': 'imbal_ctry_fact_pv', 'PV_DEF': imbal_ctry_fact_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 15, 'PV_NAME': 'StayImpCtryLevel1_pv', 'PV_DESC': 'StayImpCtryLevel1_pv', 'PV_DEF': StayImpCtryLevel1_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 16, 'PV_NAME': 'StayImpCtryLevel2_pv', 'PV_DESC': 'StayImpCtryLevel2_pv', 'PV_DEF': StayImpCtryLevel2_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 28, 'PV_NAME': 'ukport1_pv', 'PV_DESC': 'ukport1_pv', 'PV_DEF': ukport1_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 29, 'PV_NAME': 'ukport2_pv', 'PV_DESC': 'ukport2_pv', 'PV_DEF': ukport2_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 30, 'PV_NAME': 'ukport3_pv', 'PV_DESC': 'ukport3_pv', 'PV_DEF': ukport3_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 31, 'PV_NAME': 'ukport4_pv', 'PV_DESC': 'ukport4_pv', 'PV_DEF': ukport4_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 32, 'PV_NAME': 'osport1_pv', 'PV_DESC': 'osport1_pv', 'PV_DEF': osport1_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 48, 'PV_NAME': 'rail_exercise_pv', 'PV_DESC': 'rail_exercise_pv', 'PV_DEF': rail_exercise_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 49, 'PV_NAME': 'rail_imp_eligible_pv', 'PV_DESC': 'rail_imp_eligible_pv', 'PV_DEF': rail_imp_eligible_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 50, 'PV_NAME': 'spend_imp_flag_pv', 'PV_DESC': 'spend_imp_flag_pv', 'PV_DEF': spend_imp_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 51, 'PV_NAME': 'purpose_pv', 'PV_DESC': 'purpose_pv', 'PV_DEF': purpose_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 52, 'PV_NAME': 'town_imp_eligible_pv', 'PV_DESC': 'town_imp_eligible_pv', 'PV_DEF': town_imp_eligible_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 53, 'PV_NAME': 'reg_imp_eligible_pv', 'PV_DESC': 'reg_imp_eligible_pv', 'PV_DEF': reg_imp_eligible_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 54, 'PV_NAME': 'mins_ctry_grp_pv', 'PV_DESC': 'mins_ctry_grp_pv', 'PV_DEF': mins_ctry_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 55, 'PV_NAME': 'mins_port_grp_pv', 'PV_DESC': 'mins_port_grp_pv', 'PV_DEF': mins_port_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 56, 'PV_NAME': 'samp_port_grp_pv', 'PV_DESC': 'samp_port_grp_pv', 'PV_DEF': samp_port_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 57, 'PV_NAME': 'unsamp_port_grp_pv', 'PV_DESC': 'unsamp_port_grp_pv', 'PV_DEF': unsamp_port_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 4, 'PV_NAME': 'shift_flag_pv', 'PV_DESC': 'shift_flag_pv', 'PV_DEF': shift_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 5, 'PV_NAME': 'crossings_flag_pv', 'PV_DESC': 'crossings_flag_pv', 'PV_DEF': crossings_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 6, 'PV_NAME': 'shift_port_grp_pv', 'PV_DESC': 'shift_port_grp_pv', 'PV_DEF': shift_port_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 7, 'PV_NAME': 'nr_flag_pv', 'PV_DESC': 'nr_flag_pv', 'PV_DEF': nr_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 8, 'PV_NAME': 'nr_port_grp_pv', 'PV_DESC': 'nr_port_grp_pv', 'PV_DEF': nr_port_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 9, 'PV_NAME': 'mins_flag_pv', 'PV_DESC': 'mins_flag_pv', 'PV_DEF': mins_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 10, 'PV_NAME': 'imbal_eligible_pv', 'PV_DESC': 'imbal_eligible_pv', 'PV_DEF': imbal_eligible_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 11, 'PV_NAME': 'imbal_port_grp_pv', 'PV_DESC': 'imbal_port_grp_pv', 'PV_DEF': imbal_port_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 58, 'PV_NAME': 'unsamp_region_grp_pv', 'PV_DESC': 'unsamp_region_grp_pv', 'PV_DEF': unsamp_region_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 1, 'PV_NAME': 'weekday_end_pv', 'PV_DESC': 'weekday_end_pv', 'PV_DEF': weekday_end_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 2, 'PV_NAME': 'am_pm_night_pv', 'PV_DESC': 'am_pm_night_pv', 'PV_DEF': am_pm_night_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 3, 'PV_NAME': 'mig_flag_pv', 'PV_DESC': 'mig_flag_pv', 'PV_DEF': mig_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 17, 'PV_NAME': 'StayImpCtryLevel3_pv', 'PV_DESC': 'StayImpCtryLevel3_pv', 'PV_DEF': StayImpCtryLevel3_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 18, 'PV_NAME': 'StayImpCtryLevel4_pv', 'PV_DESC': 'StayImpCtryLevel4_pv', 'PV_DEF': StayImpCtryLevel4_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 19, 'PV_NAME': 'stay_purpose_grp_pv', 'PV_DESC': 'stay_purpose_grp_pv', 'PV_DEF': stay_purpose_grp_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 20, 'PV_NAME': 'fares_imp_flag_pv', 'PV_DESC': 'fares_imp_flag_pv', 'PV_DEF': fares_imp_flag_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 21, 'PV_NAME': 'fares_imp_eligible_pv', 'PV_DESC': 'fares_imp_eligible_pv', 'PV_DEF': fares_imp_eligible_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 22, 'PV_NAME': 'discnt_f1_pv', 'PV_DESC': 'discnt_f1_pv', 'PV_DEF': discnt_f1_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 23, 'PV_NAME': 'discnt_package_cost_pv', 'PV_DESC': 'discnt_package_cost_pv', 'PV_DEF': discnt_package_cost_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 24, 'PV_NAME': 'discnt_f2_pv', 'PV_DESC': 'discnt_f2_pv', 'PV_DEF': discnt_f2_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 25, 'PV_NAME': 'fage_pv', 'PV_DESC': 'fage_pv', 'PV_DEF': fage_pv},
                       {'RUN_ID': 'TEMPLATE', 'PROCESS_VARIABLE_ID': 26, 'PV_NAME': 'type_pv', 'PV_DESC': 'type_pv', 'PV_DEF': type_pv},

                   ])


def downgrade():
    op.drop_table("RUN")
    op.drop_table("PROCESS_VARIABLE_PY")
    op.drop_table("PROCESS_VARIABLE_SET")


