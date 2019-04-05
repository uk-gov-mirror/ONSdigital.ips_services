import json
import marshal

from ips.persistence.pv_builder_persistence import get_pv_build, delete_pv_build, delete_pv_bytes, store_pv_bytes, \
    create_block, create_expression, create_element
from ips.services import service


def get_index(el):
    return el[-1:]


@service
def get_pv_build_variables() -> str:
    return get_pv_build()


@service
def create_pv_build(request, run_id, pv_id=None):
    a = json.loads(request.form.get('json'))
    pv = request.form.get('pv')

    delete_pv_build(run_id, pv_id)

    setel = False
    for block in a:
        if type(a[block]) is dict:
            first = True
            s = "\n"
            block_id = create_block(run_id, get_index(block), pv_id)
            for expression in a[block]:
                expression_id = create_expression(block_id, get_index(expression))
                for element in a[block][expression]:
                    create_element(expression_id, element, a[block][expression][element])
                    if element != "var":
                        a[block][expression][element] = a[block][expression][element].lower()
                    if a[block][expression][element] == "set":
                        setel = True
                        s += ":"
                    else:
                        if setel:
                            s += "\n   "
                            setel = False
                        if not first:
                            s += " "
                        s += a[block][expression][element]
                    first = False
            pv += s
        else:
            print(a[block])
    code_obj = compile(pv, str(pv_id), 'exec')
    code_bytes = marshal.dumps(code_obj)
    delete_pv_bytes(run_id, pv_id)
    store_pv_bytes(run_id, pv_id, code_bytes)


@service
def get_pv_builds(run_id):
    res = get_pv_builds(run_id)

    arr = {}
    for row in res:
        if row['PV_ID'] not in arr.keys():
            arr[row['PV_ID']] = {}
        if row['Block_ID'] not in arr[row['PV_ID']].keys():
            arr[row['PV_ID']][row['Block_ID']] = {}
        if row['Expression_Index'] not in arr[row['PV_ID']][row['Block_ID']].keys():
            arr[row['PV_ID']][row['Block_ID']][row['Expression_Index']] = {}
        arr[row['PV_ID']][row['Block_ID']][row['Expression_Index']][row['type']] = row['content']

    builds = []
    for pv in arr:
        for block in arr[pv]:
            for expression in arr[pv][block]:
                temp = {'pv': pv, 'block': block, 'expression': expression}
                for element in arr[pv][block][expression]:
                    temp[element] = arr[pv][block][expression][element]
                builds.append(temp)

    return json.dumps(builds)
