from typing import Callable, List, TypeVar

from ips_common.ips_logging import log

import ips.services.run_management as runs
import ips.services.steps.air_miles as airmiles
import ips.services.steps.fares_imputation as fares_imputation
import ips.services.steps.final_weight as final_weight
import ips.services.steps.imbalance_weight as imbalance_weight
import ips.services.steps.minimums_weight as minimums_weight
import ips.services.steps.non_response_weight as non_response_weight
import ips.services.steps.rail_imputation as rail_imputation
import ips.services.steps.regional_weights as regional_weights
import ips.services.steps.shift_weight as shift_weight
import ips.services.steps.spend_imputation as spend_imputation
import ips.services.steps.stay_imputation as stay_imputation
import ips.services.steps.town_stay_expenditure as town_stay_expenditure
import ips.services.steps.traffic_weight as traffic_weight
import ips.services.steps.unsampled_weight as unsampled_weight
from ips.persistence.persistence import truncate_table, delete_from_table


class IPSWorkflow:
    pass


W = TypeVar('W', bound=IPSWorkflow)


def run_step(func: [[W, str], None]) -> Callable[[str], None]:
    def wrapper(self, run_id: str):
        if self.is_cancelled(run_id):
            log.info(f"Processing cancelled. Skipping step {func.__name__[1:]}")
        else:
            self.set_step_status(run_id, runs.IN_PROGRESS, func.__name__[6:])
            self.set_status(run_id, runs.IN_PROGRESS, func.__name__[1:])
            func(self, run_id)
            self.set_step_status(run_id, runs.DONE, func.__name__[6:])
    return wrapper


# noinspection PyMethodMayBeStatic
class IPSWorkflow:
    num_done: int = 0
    in_progress = False

    def __init__(self):
        log.info("IPSWorkflow starting")
        runs.clear_existing_status()

    def is_in_progress(self) -> bool:
        if self.in_progress is True:
            return True
        else:
            return False

    def cancel_run(self, run_id) -> None:
        runs.cancel_run(run_id)
        self.in_progress = False

    def is_cancelled(self, run_id) -> bool:
        return runs.is_cancelled(run_id)

    def get_status(self, run_id) -> int:
        return runs.get_status(run_id)

    def set_status(self, run_id: str, status: int, step: str) -> None:
        runs.set_status(run_id, status, step)
        if status == runs.DONE:
            self.in_progress = False
        log.debug(f"Step: {step}, status: {status}")

    def get_step_status(self, run_id: str, step: str):
        return runs.get_step_status(run_id, step)

    def set_step_status(self, run_id: str, status: int, step: str) -> None:
        runs.set_step_status(run_id, status, step)

    def reset_steps(self, run_id: str):
        runs.reset_all_step_status(run_id)

    def is_run_complete(self, run_id) -> bool:
        return runs.is_complete(run_id)

    def get_percentage_done(self, run_id) -> int:
        return runs.get_percent_done(run_id)

    def set_percent_done(self, run_id, percent):
        runs.set_percent_done(run_id, percent)

    def get_step(self, run_id):
        return runs.get_step(run_id);

    @run_step
    def _step_1(self, run_id: str) -> None:
        log.info("Calculation  1 --> shift_weight")
        shift_weight.shift_weight_step(run_id)

    @run_step
    def _step_2(self, run_id: str) -> None:
        log.info("Calculation  2 --> non_response_weight")
        non_response_weight.non_response_weight_step(run_id)

    @run_step
    def _step_3(self, run_id: str) -> None:
        log.info("Calculation  3 --> minimums_weight")
        minimums_weight.minimums_weight_step(run_id)

    @run_step
    def _step_4(self, run_id: str) -> None:
        log.info("Calculation  4 --> traffic_weight")
        traffic_weight.traffic_weight_step(run_id)

    @run_step
    def _step_5(self, run_id: str) -> None:
        log.info("Calculation  5 --> unsampled_weight")
        unsampled_weight.unsampled_weight_step(run_id)

    @run_step
    def _step_6(self, run_id: str) -> None:
        log.info("Calculation  6 --> imbalance_weight")
        imbalance_weight.imbalance_weight_step(run_id)

    @run_step
    def _step_7(self, run_id: str) -> None:
        log.info("Calculation  7 --> final_weight")
        final_weight.final_weight_step(run_id)

    @run_step
    def _step_8(self, run_id: str) -> None:
        log.info("Calculation  8 --> stay_imputation")
        stay_imputation.stay_imputation_step(run_id)

    @run_step
    def _step_9(self, run_id: str) -> None:
        log.info("Calculation  9 --> fares_imputation")
        fares_imputation.fares_imputation_step(run_id)

    @run_step
    def _step_10(self, run_id: str) -> None:
        log.info("Calculation 10 --> spend_imputation")
        spend_imputation.spend_imputation_step(run_id)

    @run_step
    def _step_11(self, run_id: str) -> None:
        log.info("Calculation 11 --> rail_imputation")
        rail_imputation.rail_imputation_step(run_id)


    @run_step
    def _step_12(self, run_id: str) -> None:
        self.set_step_status(run_id, runs.IN_PROGRESS, '12')
        regional_weights.regional_weights_step(run_id)

    @run_step
    def _step_13(self, run_id: str) -> None:
        log.info("Calculation 13 --> town_stay_expenditure_imputation")
        town_stay_expenditure.town_stay_expenditure_imputation_step(run_id)

    @run_step
    def _step_14(self, run_id: str) -> None:
        log.info("Calculation 14 --> airmiles")
        airmiles.airmiles_step(run_id)

    _dag_list: List = [
        _step_1,
        _step_2,
        _step_3,
        _step_4,
        _step_5,
        _step_6,
        _step_7,
        _step_8,
        _step_9,
        _step_10,
        _step_11,
        _step_12,
        _step_13,
        _step_14
    ]

    def _initialize(self, run_id) -> None:
        truncate_table("SAS_SURVEY_SUBSAMPLE")()
        runs.create_run(run_id)

    def run_calculations(self, run_id: str) -> None:

        try:
            self.reset_steps(run_id)
            self._initialize(run_id)
            self.num_done = 0
            self.in_progress = True

            for func in self._dag_list:
                if not self.is_cancelled(run_id):
                    func(self, run_id)
                    self.num_done += 1
                    percent = round((self.num_done / len(self._dag_list)) * 100)
                    runs.set_percent_done(run_id, percent)

        except Exception as e:
            if hasattr(e, 'message'):
                mesg = e.message
            else:
                mesg = str(e).strip("'")
            self.set_step_status(run_id, runs.FAILED, runs.get_step(run_id)[-1:])
            self.set_status(run_id, runs.FAILED, mesg)
            log.error(f"Run {run_id} has failed : {mesg}")
            runs.set_percent_done(run_id, 100)
            return

        finally:
            self.in_progress = False

        if not self.is_cancelled(run_id):
            self.set_status(run_id, runs.DONE, "DONE")
            runs.set_percent_done(run_id, 100)
