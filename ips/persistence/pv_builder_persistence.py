import json
from ips.util.services_logging import log

from ips.persistence.persistence import delete_from_table, execute_sql, get_identity, \
    insert_into_table, insert_into_table_id, read_table_values

PV_BYTES = 'PROCESS_VARIABLE_PY'

delete_bytes = delete_from_table(PV_BYTES)

run_query = execute_sql()
insert = execute_sql()
insert_into_pv_bytes = insert_into_table(PV_BYTES)


def _delete_pv_bytes(run_id, pv_id):
    delete_bytes(RUN_ID=run_id, PROCESS_VARIABLE_ID=pv_id)


def _store_pv_bytes(run_id, pv_id, code_bytes, pv_name, pv_desc):
    insert_into_pv_bytes(RUN_ID=run_id, PV_DEF=str(code_bytes), PROCESS_VARIABLE_ID=pv_id, PV_NAME=pv_name, PV_DESC=pv_desc)



def _get_index(el):
    return el[-1:]


def create_pv_build(request, run_id):
    pvs = request.get_param_as_json('json')
    for pv_id in pvs:
        pv = pvs[pv_id]['pv']
        pv_name = pvs[pv_id]['pv_name']
        pv_desc = pvs[pv_id]['pv_desc']
        _delete_pv_bytes(run_id, pv_id)
        if pv != "":
            pv = pv.replace("\\","\\\\")
            pv = pv.replace("\"", "\\\"")
            pv = pv.replace("%", "%%")
            _store_pv_bytes(run_id, pv_id, pv, pv_name, pv_desc)
