import json
import base64

from ips.persistence.persistence import delete_from_table, execute_sql, get_identity, \
    insert_into_table, read_table_values

PV_BUILDER_VARIABLES = 'G_PV_Variables'
PV_BLOCK = 'PV_Block'
PV_BYTES = 'PV_Bytes'
PV_EXPRESSION = 'PV_Expression'
PV_ELEMENT = 'PV_Element'

get_pv_build = read_table_values(PV_BUILDER_VARIABLES)

delete_pv_block = delete_from_table(PV_BLOCK)
delete_bytes = delete_from_table(PV_BYTES)

run_query = execute_sql()
insert = execute_sql()
insert_into_pv_block = insert_into_table(PV_BLOCK)
insert_into_pv_bytes = insert_into_table(PV_BYTES)
insert_into_expressions = insert_into_table(PV_EXPRESSION)
insert_into_elements = insert_into_table(PV_ELEMENT)


def get_pv_build_variables() -> str:
    data = get_pv_build()
    return data.to_json(orient='records')


def _get_pv_build_by_runid(run_id):
    get_sql = f"""
        SELECT * FROM PV_Block JOIN PV_Expression ON PV_Block.Block_ID = PV_Expression.Block_ID
        JOIN PV_Element ON PV_Expression.Expression_ID = PV_Element.Expression_ID
        JOIN G_PVs ON G_PVs.PV_ID = PV_Block.PV_ID WHERE PV_Block.Run_ID = '{run_id}'
    """

    data = run_query(get_sql)
    return data


def _create_block(run_id, index, pv_id):
    insert_into_pv_block(Run_ID=run_id, Block_Index=index, pv_id=pv_id)
    return get_identity()


def _delete_pv_build(run_id, pv_id):
    delete_pv_block(RUN_ID=run_id, PV_ID=pv_id)


def _delete_pv_bytes(run_id, pv_id):
    delete_bytes(RUN_ID=run_id, PV_ID=pv_id)


def _store_pv_bytes(run_id, pv_id, code_bytes):
    insert_into_pv_bytes(Run_ID=run_id, PV_Bytes=str(code_bytes), PV_ID=pv_id)


def _create_expression(block_id, index):
    insert_into_expressions(Block_ID=block_id, Expression_Index=index)
    return get_identity()


def _create_element(expression_id, typ, value):
    insert_into_elements(Expression_ID=expression_id, Type=typ, Content=value)


def _get_index(el):
    return el[-1:]


def create_pv_build(request, run_id, pv_id=None):
    a = request.get_param_as_json('json')
    pv = request.get_param('pv')
    _delete_pv_build(run_id, pv_id)

    setel = False
    print(a)
    for block in a:
        if type(a[block]) is dict:
            first = True
            s = "\n"
            block_id = _create_block(run_id, _get_index(block), pv_id)
            print(block_id)
            for expression in a[block]:
                expression_id = _create_expression(block_id, _get_index(expression))
                print(expression_id)
                for element in a[block][expression]:
                    print(element)
                    _create_element(expression_id, element, a[block][expression][element].replace("\"","\\\""))
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
    print(pv)
    pv_bytes = base64.b64decode(pv)
    print(pv_bytes)
    _delete_pv_bytes(run_id, pv_id)
    _store_pv_bytes(run_id, pv_id, pv_bytes)


def get_pv_builds(run_id):
    print(run_id)
    res = _get_pv_build_by_runid(run_id)
    arr = {}
    for row in res:
        print(row)
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
