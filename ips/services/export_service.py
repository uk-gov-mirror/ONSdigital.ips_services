from ips.persistence.export_data import export_from_table as export
from ips.services import service


@service
def get_export_data(run_id, table):
    return export(run_id, table)
