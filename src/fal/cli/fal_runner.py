import argparse
from pathlib import Path
from typing import Dict, List
import os

from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import raise_for_run_results_failures, run_scripts
from fal.fal_script import FalScript
from faldbt.project import DbtModel, FalDbt, FalGeneralException


def create_fal_dbt(
    args: argparse.Namespace, generated_models: Dict[str, Path] = {}
) -> FalDbt:
    real_project_dir = os.path.realpath(os.path.normpath(args.project_dir))
    real_profiles_dir = None
    env_profiles_dir = os.getenv("DBT_PROFILES_DIR")
    if args.profiles_dir is not None:
        real_profiles_dir = os.path.realpath(os.path.normpath(args.profiles_dir))
    elif env_profiles_dir:
        real_profiles_dir = os.path.realpath(os.path.normpath(env_profiles_dir))
    else:
        real_profiles_dir = DEFAULT_PROFILES_DIR

    if hasattr(args, "state") and args.state is not None:
        real_state = Path(os.path.realpath(os.path.normpath(args.state)))
    else:
        real_state = None

    return FalDbt(
        real_project_dir,
        real_profiles_dir,
        args.select,
        args.exclude,
        args.selector,
        args.keyword,
        args.threads,
        real_state,
        args.target,
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

    if args.before:
        if not _scripts_flag(args):
            # run globals when no --script is passed
            _run_global_scripts(faldbt, args.before)

        results = run_scripts(scripts, faldbt)
        raise_for_run_results_failures(scripts, results)

    else:
        results = run_scripts(scripts, faldbt)
        raise_for_run_results_failures(scripts, results)

        if not _scripts_flag(args):
            # run globals when no --script is passed
            _run_global_scripts(faldbt, args.before)


def _scripts_flag(args: argparse.Namespace) -> bool:
    return bool(args.scripts)


def _select_scripts(
    args: argparse.Namespace, models: List[DbtModel], faldbt: FalDbt
) -> List[FalScript]:
    scripts = []
    scripts_flag = _scripts_flag(args)

    for model in models:
        model_scripts = model.get_scripts(args.keyword, bool(args.before))
        for path in model_scripts:
            if not scripts_flag:
                # run all scripts when no --script is passed
                scripts.append(FalScript(faldbt, model, path))
            elif path in args.scripts:
                # if --script selector is there only run selected scripts
                scripts.append(FalScript(faldbt, model, path))

    return scripts


def _run_global_scripts(faldbt: FalDbt, is_before: bool):
    global_scripts = list(
        map(
            lambda path: FalScript(faldbt, None, path),
            faldbt._global_script_paths["before" if is_before else "after"],
        )
    )

    results = run_scripts(global_scripts, faldbt)
    raise_for_run_results_failures(global_scripts, results)


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
            if node.get_scripts(faldbt.keyword, before) != []:
                filtered_models.append(node)
        elif all:
            filtered_models.append(node)
        elif node.status != "skipped":
            filtered_models.append(node)

    return filtered_models


def _models_ids(models):
    return list(map(lambda r: r.unique_id, models))
