import ips.persistence.pv_builder_persistence as builder
from ips.services import service


@service
def get_pv_build_variables() -> str:
    return builder.get_pv_build_variables()


@service
def create_pv_build(request, run_id, pv_id=None):
    builder.create_pv_build(request, run_id, pv_id)


@service
def get_pv_builds(run_id):
    return builder.get_pv_builds(run_id)
