"""Run fal scripts."""
import sys
from typing import List

from multiprocessing.pool import Pool
from multiprocessing.dummy import Pool as ThreadPool

from dbt.logger import GLOBAL_LOGGER as logger

from faldbt.project import FalDbt
from fal.fal_script import FalScript
from fal.utils import print_run_info


import traceback


def _prepare_exec_script(script: FalScript, faldbt: FalDbt) -> bool:
    success: bool = True

    logger.debug("Running script {}", script.id)

    try:
        script.exec(faldbt)
    except:
        logger.error("Error in script {}:\n{}", script.id, traceback.format_exc())
        # TODO: what else to do?
        success = False

    finally:
        logger.debug("Finished script {}", script.id)

    return success


def raise_for_run_results_failures(scripts: List[FalScript], results: List[bool]):
    if not all(results):
        failures = filter(lambda t: not t[1], zip(scripts, results))
        failure_ids = map(lambda t: t[0].id, failures)

        raise RuntimeError(f"Error in scripts {str.join(', ', failure_ids)}")


def run_scripts(scripts: List[FalScript], faldbt: FalDbt) -> List[bool]:

    print_run_info(scripts)

    # Enable local imports for fal scripts
    sys.path.append(faldbt.scripts_dir)

    logger.info("Concurrency: {} threads", faldbt.threads)
    with ThreadPool(faldbt.threads) as pool:
        pool: Pool = pool
        try:
            scripts_with_project = map(lambda script: (script, faldbt), scripts)
            results = pool.starmap(_prepare_exec_script, scripts_with_project)

        except KeyboardInterrupt:
            pool.close()
            pool.terminate()
            pool.join()
            raise

    logger.debug("Script results: {}", results)

    sys.path.remove(faldbt.scripts_dir)
    return results
