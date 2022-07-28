import argparse
from itertools import chain
from pathlib import Path
from pydoc import ModuleScanner
from typing import Dict, List, Iterable
import os

from dbt.config.profile import DEFAULT_PROFILES_DIR
from fal.planner.executor import parallel_executor
from fal.planner.schedule import Scheduler
from fal.planner.tasks import FalHookTask, Status, TaskGroup

from fal.fal_script import FalScript
from faldbt.project import DbtModel, FalDbt, FalGeneralException


def create_fal_dbt(
    args: argparse.Namespace, generated_models: Dict[str, Path] = {}
) -> FalDbt:
    env_profiles_dir = os.getenv("DBT_PROFILES_DIR")

    profiles_dir = DEFAULT_PROFILES_DIR
    if args.profiles_dir is not None:
        profiles_dir = args.profiles_dir
    elif env_profiles_dir:
        profiles_dir = env_profiles_dir

    real_state = None
    if hasattr(args, "state") and args.state is not None:
        real_state = args.state

    return FalDbt(
        args.project_dir,
        profiles_dir,
        args.select,
        args.exclude,
        args.selector,
        args.keyword,
        args.threads,
        real_state,
        args.target,
        getattr(args, "vars", "{}"),
        generated_models,
    )


def fal_run(args: argparse.Namespace):
    "Runs the fal run command in a subprocess"

    selector_flags = args.select or args.exclude or args.selector
    if args.all and selector_flags:
        raise FalGeneralException(
            "Cannot pass --all flag alongside selection flags (--select/--models, --exclude, --selector)"
        )

    faldbt = create_fal_dbt(args)
    models = _get_filtered_models(faldbt, args.all, selector_flags, args.before)

    scripts = _select_scripts(args, models, faldbt)

    global_scripts = _get_global_scripts(faldbt, args.before)

    if args.before:
        if not _scripts_flag(args):
            # run globals when no --script is passed
            _run_scripts(args, global_scripts, faldbt)

        pre_hook_scripts = _get_hooks_for_model(models, faldbt, "pre-hook")
        _run_scripts(args, pre_hook_scripts, faldbt)

        _run_scripts(args, scripts, faldbt)

    else:
        _run_scripts(args, scripts, faldbt)

        post_hook_scripts = _get_hooks_for_model(models, faldbt, "post-hook")
        _run_scripts(args, post_hook_scripts, faldbt)

        if not _scripts_flag(args):
            # run globals when no --script is passed
            _run_scripts(args, global_scripts, faldbt)


def _run_scripts(args: argparse.Namespace, scripts: List[FalScript], faldbt: FalDbt):
    scheduler = Scheduler(
        [TaskGroup(FalHookTask.from_fal_script(script)) for script in scripts]
    )
    parallel_executor(args, faldbt, scheduler)

    failed_tasks: List[FalHookTask] = [
        group.task for group in scheduler.filter_groups(Status.FAILURE)
    ]  # type: ignore
    failed_script_ids = [task.build_fal_script(faldbt).id for task in failed_tasks]
    if failed_script_ids:
        raise RuntimeError(f"Error in scripts {str.join(', ',failed_script_ids)}")


def _scripts_flag(args: argparse.Namespace) -> bool:
    return bool(args.scripts)


def _get_hooks_for_model(
    models: List[DbtModel], faldbt: FalDbt, hook_type: str
) -> List[FalScript]:
    return [
        FalScript(faldbt, model, path, True)
        for model in models
        for path in model._get_hook_paths(hook_type=hook_type)
    ]


def _select_scripts(
    args: argparse.Namespace, models: List[DbtModel], faldbt: FalDbt
) -> List[FalScript]:
    scripts = []
    scripts_flag = _scripts_flag(args)

    for model in models:
        model_scripts = model.get_scripts(args.keyword, before=bool(args.before))
        for path in model_scripts:
            if not scripts_flag:
                # run all scripts when no --script is passed
                scripts.append(FalScript(faldbt, model, path))
            elif path in args.scripts:
                # if --script selector is there only run selected scripts
                scripts.append(FalScript(faldbt, model, path))

    return scripts


def _get_global_scripts(faldbt: FalDbt, is_before: bool):
    return [
        FalScript(faldbt, None, path)
        for path in faldbt._global_script_paths["before" if is_before else "after"]
    ]


def _get_models_with_keyword(faldbt: FalDbt) -> List[DbtModel]:
    return list(
        filter(lambda model: faldbt.keyword in model.meta, faldbt.list_models())
    )


def _get_filtered_models(faldbt: FalDbt, all, selected, before) -> List[DbtModel]:
    selected_ids = _models_ids(faldbt._compile_task._flattened_nodes)
    filtered_models: List[DbtModel] = []

    if (
        not all
        and not selected
        and not before
        and faldbt._run_results.nativeRunResult is None
    ):
        from faldbt.parse import FalParseError

        raise FalParseError(
            "Cannot define models to run without selection flags or dbt run_results artifact or --before flag"
        )

    models = _get_models_with_keyword(faldbt)

    for node in models:
        if selected:
            if node.unique_id in selected_ids:
                filtered_models.append(node)
        elif before:
            if node.get_scripts(faldbt.keyword, before=before) != []:
                filtered_models.append(node)
        elif all:
            filtered_models.append(node)
        elif node.status != "skipped":
            filtered_models.append(node)

    return filtered_models


def _models_ids(models):
    return list(map(lambda r: r.unique_id, models))
