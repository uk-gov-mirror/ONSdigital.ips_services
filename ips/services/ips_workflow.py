import multiprocessing
import os
from typing import Dict, Callable, List, TypeVar

from ips_common.ips_logging import log

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
from ips.persistence.persistence import clear_memory_table


class IPSWorkflow:
    pass


W = TypeVar('W', bound=IPSWorkflow)


def run_step(func: [[W, str], None]) -> Callable[[str], None]:
    def wrapper(self, run_id: str):
        if self.is_cancelled():
            self.set_status(func.__name__[1:], self._CANCELLED)
            log.info(f"Processing cancelled. Skipping step {func.__name__[1:]}")
        else:
            self.set_status(func.__name__[1:], self._IN_PROGRESS)
            func(self, run_id)
            self.set_status(func.__name__[1:], self._DONE)

    return wrapper


class IPSWorkflow:
    _current_status: Dict[str, int]
    _NOT_STARTED: int = 1
    _IN_PROGRESS: int = 2
    _DONE: int = 3
    _CANCELLED: int = 4

    _in_progress: bool = False
    _cancel_run: bool = False

    def __init__(self):
        self._current_status = {}

    def in_progress(self) -> bool:
        return self._in_progress

    def cancel_run(self) -> None:
        self._cancel_run = True
        self._in_progress = False

    def is_cancelled(self) -> bool:
        return self._cancel_run

    def get_status(self) -> Dict[str, int]:
        return self._current_status

    def set_status(self, step: str, status: int) -> None:
        self._current_status[step] = status
        log.info(f"Step: {step}, status: {status}")

    def run_complete(self) -> bool:
        if len(self._current_status.keys()) != 14:
            return False
        for _, value in self._current_status.items():
            if value != IPSWorkflow._DONE and value != IPSWorkflow._CANCELLED:
                return False
        return True

    def get_percentage_done(self) -> int:
        num_done = 0
        for key, value in self._current_status.items():
            if value == IPSWorkflow._DONE or value == IPSWorkflow._CANCELLED:
                num_done += 1
        return round((num_done / 14) * 100)

    def _run_steps(self, func_list: List, run_id: str, parallel: bool = False) -> None:
        def run_parallel():
            num_partitions = len(func_list)
            pool = multiprocessing.Pool(num_partitions)

            for func in func_list:
                # f = partial(func, run_id)
                pool.map(func, run_id)

            pool.close()
            pool.join()

        def run_serial() -> None:
            for func in func_list:
                func(self, run_id)

        if parallel:
            run_parallel()
        else:
            run_serial()

    @run_step
    def _step_1(self, run_id: str) -> None:
        log.info(f"Calculation 1, [shift_weight calculation], process id: {os.getpid()}")
        shift_weight.shift_weight_step(run_id)

    @run_step
    def _step_2(self, run_id: str) -> None:
        log.info(f"Calculation 2, [non_response_weight_step], process id: {os.getpid()}")
        non_response_weight.non_response_weight_step(run_id)

    @run_step
    def _step_3(self, run_id: str) -> None:
        log.info(f"Calculation 3, [minimums_weight_step], process id: {os.getpid()}")
        minimums_weight.minimums_weight_step(run_id)

    @run_step
    def _step_4(self, run_id: str) -> None:
        log.info(f"Calculation 4, [traffic_weight_step], process id: {os.getpid()}")
        traffic_weight.traffic_weight_step(run_id)

    @run_step
    def _step_5(self, run_id: str) -> None:
        log.info(f"Calculation 5, [unsampled_weight_step], process id: {os.getpid()}")
        unsampled_weight.unsampled_weight_step(run_id)

    @run_step
    def _step_6(self, run_id: str) -> None:
        log.info(f"Calculation 6, [imbalance_weight_step], process id: {os.getpid()}")
        imbalance_weight.imbalance_weight_step(run_id)

    @run_step
    def _step_7(self, run_id: str) -> None:
        log.info(f"Calculation 7,  [final_weight_step], process id: {os.getpid()}")
        final_weight.final_weight_step(run_id)

    @run_step
    def _step_8(self, run_id: str) -> None:
        log.info(f"Calculation 8, [stay_imputation_step], process id: {os.getpid()}")
        stay_imputation.stay_imputation_step(run_id)

    @run_step
    def _step_9(self, run_id: str) -> None:
        log.info(f"Calculation 9, [fares_imputation_step], process id: {os.getpid()}")
        fares_imputation.fares_imputation_step(run_id)

    @run_step
    def _step_10(self, run_id: str) -> None:
        log.info(f"Calculation 10, [spend_imputation_step], process id: {os.getpid()}")
        spend_imputation.spend_imputation_step(run_id)

    @run_step
    def _step_11(self, run_id: str) -> None:
        log.info(f"Calculation 11, [rail_imputation_step], process id: {os.getpid()}")
        rail_imputation.rail_imputation_step(run_id)

    @run_step
    def _step_12(self, run_id: str) -> None:
        log.info(f"Calculation 12, [regional_weights_step] process id: {os.getpid()}")
        regional_weights.regional_weights_step(run_id)

    @run_step
    def _step_13(self, run_id: str) -> None:
        log.info(f"Calculation 13, [town_stay_expenditure_imputation_step], process id: {os.getpid()}")
        town_stay_expenditure.town_stay_expenditure_imputation_step(run_id)

    @run_step
    def _step_14(self, run_id: str) -> None:
        log.info(f"Calculation 14, [airmiles.airmiles_step], process id: {os.getpid()}")
        airmiles.airmiles_step(run_id)

    _dag_list: List = {
        1: [_step_1, _step_8, _step_9, _step_14],
        2: [_step_2, _step_3, _step_10],
        3: [_step_4],
        4: [_step_5],
        5: [_step_6],
        6: [_step_7],
        7: [_step_11],
        8: [_step_13],
        9: [_step_12]
    }

    def _initialize(self) -> None:
        clear_memory_table("SURVEY_SUBSAMPLE")()
        clear_memory_table("SAS_SURVEY_SUBSAMPLE")()

        self._current_status = {}
        for x in range(14):
            self._current_status["step_" + str(x + 1)] = IPSWorkflow._NOT_STARTED
        log.info("Cleared current_status")

    def run_calculations(self, run_id: str) -> None:
        self._initialize()
        self._in_progress = True

        for x in self._dag_list:
            lst = self._dag_list[x]
            log.info(f"--> Start Step: {x}")
            self._run_steps(lst, run_id)
            log.info(f"--> End Step: {x}\n")

        self._in_progress = False
