import re
from typing import List
from dataclasses import dataclass
from fal.node_graph import NodeGraph
from faldbt.project import CompileArgs, FalDbt
from dbt.task.compile import CompileTask
from enum import Enum
import faldbt.lib as lib


class ExecutionPlan:
    """
    Represents a fal flow excution
    """

    before_scripts: List[str]
    dbt_models: List[str]
    after_scripts: List[str]
    project_name: str

    def __init__(self, unique_ids: List[str], project_name):
        self.before_scripts = []
        self.dbt_models = []
        self.after_scripts = []
        self.project_name = project_name
        for id in unique_ids:
            if _is_before_scipt(id):
                self.before_scripts.append(id)
            elif _is_after_script(id):
                self.after_scripts.append(id)
            else:
                self.dbt_models.append(id)

    @classmethod
    def create_plan_from_graph(
        cls, parsed, nodeGraph: NodeGraph, project_name: str, fal_dbt: FalDbt
    ):
        """
        Creates and ExecutionPlan from the cli arguments
        """
        unique_ids = list(nodeGraph.graph.nodes.keys())
        ids_to_execute = []
        if parsed.select:
            selector_plans = list(
                map(
                    lambda selector: SelectorPlan(
                        selector, unique_ids, project_name, fal_dbt
                    ),
                    list(parsed.select),
                )
            )
            for selector_plan in selector_plans:
                for id in selector_plan.unique_ids:
                    ids_to_execute.append(id)
                    if selector_plan.children:
                        children = list(nodeGraph.get_descendants(id))
                        ids_to_execute.extend(children)
                    if selector_plan.parents:
                        parents = list(nodeGraph.get_predecessors(id))
                        ids_to_execute.extend(parents)

        else:
            ids_to_execute.extend(unique_ids)
        return cls(list(set(ids_to_execute)), project_name)


def _expand_script(script_name: str, unique_ids: List[str]) -> List[str]:
    """
    Expands the selected script name to unique id format.
    for example [scripta.py] to [script.modelB.AFTER.scripta.py, script.modelA.BEFORE.scripta.py]
    """

    def contains_script_name(id: str):
        return script_name in id

    return list(filter(contains_script_name, unique_ids))


class SelectType(Enum):
    MODEL = 1
    SCRIPT = 2
    COMPLEX = 3


class SelectorPlan:
    """
    Represents a single selector, for example in the command

    fal flow run --select script.py+

    script.py+ is the SelectorPlan with needs_children attribute set to true
    """

    unique_ids: List[str]
    children: bool
    parents: bool
    type: SelectType

    def __init__(
        self, selector: str, unique_ids: List[str], project_name, fal_dbt: FalDbt
    ):
        self.children = _needs_children(selector)
        self.parents = _need_parents(selector)
        self.type = _to_select_type(selector)
        node_name = _remove_graph_selectors(selector)

        if self.type == SelectType.MODEL:
            self.unique_ids = [f"model.{project_name}.{node_name}"]
        elif self.type == SelectType.SCRIPT:
            self.unique_ids = _expand_script(node_name, unique_ids)
        elif self.type == SelectType.COMPLEX:
            self.unique_ids = unique_ids_from_complex_selector(selector, fal_dbt)


def unique_ids_from_complex_selector(select, fal_dbt: FalDbt) -> List[str]:
    args = CompileArgs(None, [select], [select], tuple(), fal_dbt._state, None)
    compile_task = CompileTask(args, fal_dbt._config)
    compile_task._runtime_initialize()
    spec = compile_task.get_selection_spec()
    graph = compile_task.get_node_selector().get_graph_queue(spec)
    return list(graph.queued)


def _to_select_type(select: str) -> SelectType:
    if ":" in select:
        return SelectType.COMPLEX
    else:
        node_name = _remove_graph_selectors(select)
        if _is_script_node(node_name):
            return SelectType.SCRIPT
        else:
            return SelectType.MODEL


def _is_script_node(node_name: str) -> bool:
    return node_name.endswith(".py")


def _remove_graph_selectors(selector: str) -> str:
    return selector.replace("+", "")


def _needs_children(selector: str) -> bool:
    children_operation_regex = re.compile(".*\\+$")
    return bool(children_operation_regex.match(selector))


def _need_parents(selector: str) -> bool:
    parent_operation_regex = re.compile("^\\+.*")
    return bool(parent_operation_regex.match(selector))


def _is_before_scipt(id: str) -> bool:
    before_script_regex = re.compile("script.*.BEFORE.*.py")
    return bool(before_script_regex.match(id))


def _is_after_script(id: str) -> bool:
    after_script_regex = re.compile("script.*.AFTER.*.py")
    return bool(after_script_regex.match(id))
