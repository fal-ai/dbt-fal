import re
from typing import List
from dataclasses import dataclass
from fal.node_graph import NodeGraph


class ExecutionPlan:
    """
    Represents a fal flow excution
    """

    before_scripts: List[str]
    dbt_models: List[str]
    after_scripts: List[str]

    def __init__(self, unique_ids: List[str]):
        self.before_scripts = []
        self.dbt_models = []
        self.after_scripts = []
        for id in unique_ids:
            if _is_before_scipt(id):
                self.before_scripts.append(id)
            elif _is_after_script(id):
                self.after_scripts.append(id)
            else:
                self.dbt_models.append(id)

    @classmethod
    def create_plan_from_graph(cls, parsed, nodeGraph: NodeGraph):
        """
        Creates and ExecutionPlan from the cli arguments
        """
        unique_ids = list(nodeGraph.graph.nodes.keys())
        ids_to_execute = []
        if parsed.select:
            selector_plans = list(
                map(
                    lambda selector: SelectorPlan(selector, unique_ids),
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
        return cls(list(set(ids_to_execute)))


def _expand_script(script_name: str, unique_ids: List[str]) -> List[str]:
    """
    Expands the selected script name to unique id format.
    for example [scripta.py] to [script.modelB.AFTER.scripta.py, script.modelA.BEFORE.scripta.py]
    """

    def contains_script_name(id: str):
        return script_name in id

    return list(filter(lambda id: contains_script_name(id), unique_ids))


class SelectorPlan:
    """
    Represents a single selector, for example in the command

    fal flow run --select script.py+

    script.py+ is the SelectorPlan with needs_children attribute set to true
    """

    unique_ids: List[str]
    children: bool
    parents: bool

    def __init__(self, selector: str, unique_ids: List[str]):
        self.children = _needs_children(selector)
        self.parents = _need_parents(selector)
        node_name = _remove_graph_selectors(selector)
        if _is_script_node(node_name):
            self.unique_ids = _expand_script(node_name, unique_ids)
        else:
            self.unique_ids = ["model." + node_name]


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
