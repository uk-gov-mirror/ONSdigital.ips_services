from RestrictedPython import compile_restricted_exec
from RestrictedPython import safe_builtins
import ips.persistence.sql as db
import data.PVs as pv
import pandas
from ips.util.services_logging import log

# noinspection PyUnresolvedReferences
import math
# noinspection PyUnresolvedReferences
from datetime import datetime
# noinspection PyUnresolvedReferences
import numpy as np

column_names = db.get_column_names("SAS_SURVEY_SUBSAMPLE")
column_names.append('PORTROUTE')
df = pandas.DataFrame(columns=column_names)

for a in column_names:
    df[a] = [0]




def getitem(object, name, default=None):  # known special case of getitem
    """
    extend to ensure only specific items can be accessed if required
    """
    return object[name]


def _write_wrapper():
    # Construct the write wrapper class
    def _handler(secattr, error_msg):
        # Make a class method.
        def handler(self, *args):
            try:
                f = getattr(self.ob, secattr)
            except AttributeError:
                raise TypeError(error_msg)
            f(*args)

        return handler

    class Wrapper(object):
        def __init__(self, ob):
            self.__dict__['ob'] = ob

        __setitem__ = _handler(
            '__guarded_setitem__',
            'object does not support item or slice assignment')

        __delitem__ = _handler(
            '__guarded_delitem__',
            'object does not support item or slice assignment')

        __setattr__ = _handler(
            '__guarded_setattr__',
            'attribute-less object (assign or del)')

        __delattr__ = _handler(
            '__guarded_delattr__',
            'attribute-less object (assign or del)')

    return Wrapper


def _write_guard():
    # Nested scope abuse!
    # safetypes and Wrapper variables are used by guard()
    safetypes = {dict, list, pandas.DataFrame, pandas.Series}
    Wrapper = _write_wrapper()

    def guard(ob):
        # Don't bother wrapping simple types, or objects that claim to
        # handle their own write security.
        if type(ob) in safetypes or hasattr(ob, '_guarded_writes'):
            return ob
        # Hand the object to the Wrapper instance, then return the instance.
        return Wrapper(ob)

    return guard


write_guard = _write_guard()

safe_globals = dict(__builtins__=safe_builtins)

safe_globals['_getitem_'] = getitem
safe_globals['_getattr_'] = getattr
safe_globals['_write_'] = write_guard
safe_globals['math'] = math
safe_globals['datetime'] = datetime


def exec_code(row, dataset, pvs):
    log.debug(f"exec PV: pv['PROCVAR_NAME']")
    for pv in pvs:
        safe_globals['row'] = row
        safe_globals['dataset'] = dataset
        code = pv['PROCVAR_RULE']
        try:
            exec(code, safe_globals, None)
        except ValueError:
            name = pv['PROCVAR_NAME']
            log.error(f"ValueError on PV: {name}")
            raise ValueError

        except KeyError:
            name = pv['PROCVAR_NAME']
            log.error(f"KeyError on PV: {name}")
            raise KeyError

        except TypeError:
            name = pv['PROCVAR_NAME']
            log.error(f"TypeError on PV: {name}")
            raise TypeError

        except SyntaxError:
            name = pv['PROCVAR_NAME']
            log.error(f"SyntaxError on PV: {name}")
            raise SyntaxError


def compile_pvs(pv_list):
    for a in pv_list:
        log.debug(f"Compiling PV: {a['PROCVAR_NAME']}")
        a['PROCVAR_RULE'] = compile_restricted_exec(
            a['PROCVAR_RULE'],
            filename=a['PROCVAR_NAME']
        )


def main():
    pvs = [
        {'PROCVAR_NAME': 'am_pm_night_pv', 'PROCVAR_RULE': pv.am_pm_night_pv},
        {'PROCVAR_NAME': 'apd_pv', 'PROCVAR_RULE': pv.apd_pv},
        {'PROCVAR_NAME': 'type_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'fage_pv', 'PROCVAR_RULE': pv.fage_pv},
        {'PROCVAR_NAME': 'discnt_f2_pv', 'PROCVAR_RULE': pv.discnt_f2_pv},
        {'PROCVAR_NAME': 'discnt_package_cost', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'discnt_f1_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'fares_imp_eligible_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'StayImpCtryLevel4_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'StayImpCtryLevel3_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'mig_flag_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'am_pm_night_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'weekday_end_pv', 'PROCVAR_RULE': pv.type_pv},
        {'PROCVAR_NAME': 'unsamp_region_grp_pv', 'PROCVAR_RULE': pv.unsamp_region_grp_pv},
        {'PROCVAR_NAME': 'imbal_port_grp_pv', 'PROCVAR_RULE': pv.imbal_port_grp_pv},
        {'PROCVAR_NAME': 'imbal_eligible_pv', 'PROCVAR_RULE': pv.imbal_eligible_pv},
        {'PROCVAR_NAME': 'mins_flag_pv', 'PROCVAR_RULE': pv.mins_flag_pv},
        {'PROCVAR_NAME': 'nr_port_grp_pv', 'PROCVAR_RULE': pv.nr_port_grp_pv},
        {'PROCVAR_NAME': 'nr_flag_pv', 'PROCVAR_RULE': pv.nr_flag_pv},
        {'PROCVAR_NAME': 'shift_port_grp_pv', 'PROCVAR_RULE': pv.shift_port_grp_pv},
        {'PROCVAR_NAME': 'crossings_flag_pv', 'PROCVAR_RULE': pv.crossings_flag_pv},
        {'PROCVAR_NAME': 'shift_flag_pv', 'PROCVAR_RULE': pv.shift_flag_pv},
        {'PROCVAR_NAME': 'unsamp_port_grp_pv', 'PROCVAR_RULE': pv.unsamp_port_grp_pv},
        {'PROCVAR_NAME': 'samp_port_grp_pv', 'PROCVAR_RULE': pv.samp_port_grp_pv},
        {'PROCVAR_NAME': 'mins_port_grp_pv', 'PROCVAR_RULE': pv.mins_port_grp_pv},
        {'PROCVAR_NAME': 'mins_ctry_grp_pv', 'PROCVAR_RULE': pv.mins_ctry_grp_pv},
        {'PROCVAR_NAME': 'reg_imp_eligible_pv', 'PROCVAR_RULE': pv.reg_imp_eligible_pv},
        {'PROCVAR_NAME': 'town_imp_eligible_pv', 'PROCVAR_RULE': pv.town_imp_eligible_pv},
        {'PROCVAR_NAME': 'purpose_pv', 'PROCVAR_RULE': pv.purpose_pv},
        {'PROCVAR_NAME': 'spend_imp_flag_pv', 'PROCVAR_RULE': pv.spend_imp_flag_pv},
        {'PROCVAR_NAME': 'rail_imp_eligible_pv', 'PROCVAR_RULE': pv.rail_imp_eligible_pv},
        {'PROCVAR_NAME': 'rail_exercise_pv', 'PROCVAR_RULE': pv.rail_exercise_pv},
        {'PROCVAR_NAME': 'osport1_pv', 'PROCVAR_RULE': pv.osport1_pv},
        {'PROCVAR_NAME': 'ukport4_pv', 'PROCVAR_RULE': pv.ukport4_pv},
        {'PROCVAR_NAME': 'ukport3_pv', 'PROCVAR_RULE': pv.ukport3_pv},
        {'PROCVAR_NAME': 'ukport2_pv', 'PROCVAR_RULE': pv.ukport2_pv},
        {'PROCVAR_NAME': 'ukport1_pv', 'PROCVAR_RULE': pv.ukport1_pv},
        {'PROCVAR_NAME': 'StayImpCtryLevel2_pv', 'PROCVAR_RULE': pv.StayImpCtryLevel2_pv},
        {'PROCVAR_NAME': 'StayImpCtryLevel1_pv', 'PROCVAR_RULE': pv.StayImpCtryLevel1_pv},
        {'PROCVAR_NAME': 'imbal_ctry_fact_pv', 'PROCVAR_RULE': pv.imbal_ctry_fact_pv},
        {'PROCVAR_NAME': 'dur2_pv', 'PROCVAR_RULE': pv.dur2_pv},
        {'PROCVAR_NAME': 'dur1_pv', 'PROCVAR_RULE': pv.dur1_pv},
        {'PROCVAR_NAME': 'pur3_pv', 'PROCVAR_RULE': pv.pur3_pv},
        {'PROCVAR_NAME': 'pur2_pv', 'PROCVAR_RULE': pv.pur2_pv},
        {'PROCVAR_NAME': 'pur1_pv', 'PROCVAR_RULE': pv.pur1_pv},
        {'PROCVAR_NAME': 'uk_os_pv', 'PROCVAR_RULE': pv.uk_os_pv},
        {'PROCVAR_NAME': 'spend_imp_eligible_pv', 'PROCVAR_RULE': pv.spend_imp_eligible_pv},
        {'PROCVAR_NAME': 'duty_free_pv', 'PROCVAR_RULE': pv.duty_free_pv},
        {'PROCVAR_NAME': 'qmfare_pv', 'PROCVAR_RULE': pv.qmfare_pv},
        {'PROCVAR_NAME': 'apd_pv', 'PROCVAR_RULE': pv.apd_pv},
        {'PROCVAR_NAME': 'osport4_pv', 'PROCVAR_RULE': pv.osport4_pv},
        {'PROCVAR_NAME': 'osport3_pv', 'PROCVAR_RULE': pv.osport3_pv},
        {'PROCVAR_NAME': 'rail_cntry_grp_pv', 'PROCVAR_RULE': pv.rail_cntry_grp_pv},
        {'PROCVAR_NAME': 'osport2_pv', 'PROCVAR_RULE': pv.osport2_pv},
        {'PROCVAR_NAME': 'stay_imp_eligible_pv', 'PROCVAR_RULE': pv.stay_imp_eligible_pv},
        {'PROCVAR_NAME': 'stay_imp_flag_pv', 'PROCVAR_RULE': pv.stay_imp_flag_pv},
        {'PROCVAR_NAME': 'imbal_port_fact_pv', 'PROCVAR_RULE': pv.imbal_port_fact_pv}
    ]

    compile_pvs(pvs)
    survey = "survey"
    df.apply(exec_code, axis=1, dataset=survey, pvs=pvs)


# TODO: Load up survey subsample into the DF
if __name__ == "__main__":
    main()
