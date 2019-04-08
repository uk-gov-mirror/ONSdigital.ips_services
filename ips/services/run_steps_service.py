import ips.persistence.run_steps as steps
from ips.services import service


@service
def get_run_steps(run_id: str = None) -> str:
    return steps.get_run_steps(run_id)


@service
def create_run_steps(run_id: str) -> None:
    steps.create_run_steps(run_id)


@service
def edit_run_steps(run_id: str, value: str, step_number: str = None) -> None:
    steps.edit_run_steps(run_id, value, step_number)
