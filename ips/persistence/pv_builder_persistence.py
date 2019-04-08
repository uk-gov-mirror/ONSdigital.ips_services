from ips.persistence.persistence import delete_from_table, execute_sql, get_identity, \
    insert_into_table, read_table_values

PV_BUILDER_VARIABLES = 'G_PV_Variables'
PV_BLOCK = 'PV_Block'
PV_BYTES = 'PV_BYTES'
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


def get_pv_build_by_runid(run_id):
    get_sql = f"""
        SELECT * FROM PV_Block JOIN PV_Expression ON PV_Block.Block_ID = PV_Expression.Block_ID
        JOIN PV_Element ON PV_Expression.Expression_ID = PV_Element.Expression_ID
        JOIN G_PVs ON G_PVs.PV_ID = PV_Block.PV_ID WHERE PV_Block.Run_ID = '{run_id}'
    """

    return run_query(get_sql)


def create_block(run_id, index, pv_id):
    insert_into_pv_block(Run_ID=run_id, Block_Index=index, pv_id=pv_id)
    return get_identity()


def delete_pv_build(run_id, pv_id):
    delete_pv_block(RUN_ID=run_id, PV_ID=pv_id)


def delete_pv_bytes(run_id, pv_id):
    delete_bytes(RUN_ID=run_id, PV_ID=pv_id)


def store_pv_bytes(run_id, pv_id, code_bytes):
    insert_into_pv_bytes(Run_ID=run_id, PV_Bytes=str(code_bytes), PV_ID=pv_id)


def create_expression(block_id, index):
    insert_into_expressions(Block_ID=block_id, Expression_Index=index)
    return get_identity()


def create_element(expression_id, typ, value):
    insert_into_elements(Expression_ID=expression_id, Type=typ, Content=value)

