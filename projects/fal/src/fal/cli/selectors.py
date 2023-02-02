import itertools
import re
from dataclasses import dataclass
from typing import List, Optional, Union, Iterator
from fal.node_graph import NodeGraph
from faldbt.project import CompileArgs, FalDbt
from dbt.task.compile import CompileTask
from enum import Enum
from functools import reduce
import networkx as nx


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
            if _is_before_script(id):
                self.before_scripts.append(id)
            elif _is_after_script(id):
                self.after_scripts.append(id)
            else:
                self.dbt_models.append(id)

    @property
    def nodes(self) -> List[str]:
        return self.before_scripts + self.after_scripts + self.dbt_models

    @classmethod
    def create_plan_from_graph(cls, parsed, nodeGraph: NodeGraph, fal_dbt: FalDbt):
        """
        Creates and ExecutionPlan from the cli arguments
        """
        unique_ids = list(nodeGraph.graph.nodes.keys())

        ids_to_execute = unique_ids
        if parsed.select:
            ids_to_execute = _filter_node_ids(
                unique_ids, fal_dbt, list(parsed.select), nodeGraph
            )

        ids_to_exclude = []
        if "exclude" in parsed and parsed.exclude:
            ids_to_exclude = _filter_node_ids(
                unique_ids, fal_dbt, list(parsed.exclude), nodeGraph
            )

        ids_to_execute = [i for i in ids_to_execute if i not in ids_to_exclude]

        # Remove non-model nodes (sources, maybe more?) by making sure they are in the node_lookup dict
        ids_to_execute = [i for i in ids_to_execute if i in nodeGraph.node_lookup]
        return cls(list(set(ids_to_execute)), fal_dbt.project_name)


@dataclass
class SelectionUnion:
    components: List[str]


@dataclass
class SelectionIntersection:
    components: List[str]


def parse_union(
    components: List[str],
) -> SelectionUnion:
    # Based on the original implemention at dbt-core.

    # turn ['a b', 'c'] -> ['a', 'b', 'c']
    raw_specs = itertools.chain.from_iterable(r.split(OP_SET_UNION) for r in components)
    union_components = []

    # ['a', 'b', 'c,d'] -> union('a', 'b', intersection('c', 'd'))
    for raw_spec in raw_specs:
        union_components.append(
            SelectionIntersection(
                raw_spec.split(OP_SET_INTERSECTION),
            )
        )
    return SelectionUnion(
        union_components,
    )


def _filter_node_ids(
    unique_ids: List[str],
    fal_dbt: FalDbt,
    selectors: List[str],
    nodeGraph: NodeGraph,
) -> List[str]:
    """Filter list of unique_ids according to a selector."""
    output = set()

    union = parse_union(selectors)
    for intersection in union.components:
        try:
            plan_outputs = [
                set(SelectorPlan(selector, unique_ids, fal_dbt).execute(nodeGraph))
                for selector in intersection.components
                if selector
            ]
        except nx.NetworkXError:
            # When the user selects a non-existent node, don't fail immediately
            # but rather just continue processing the rest of selectors.
            plan_outputs = []

        if plan_outputs:
            output |= set.intersection(*plan_outputs)

    return list(output)


def _get_children_with_parents(node_id: str, nodeGraph: NodeGraph) -> List[str]:
    children = nodeGraph.get_descendants(node_id)
    output = reduce(lambda l, ch: l + nodeGraph.get_ancestors(ch), children, children)

    output = list(set(output))

    return output


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


@dataclass(init=False)
class SelectorPlan:
    """
    Represents a single selector, for example in the command

    fal flow run --select script.py+

    script.py+ is the SelectorPlan with needs_children attribute set to true
    """

    unique_ids: List[str]
    children: bool
    children_levels: Optional[int]
    children_with_parents: bool
    parents: bool
    parents_levels: Optional[int]
    type: SelectType
    raw: str

    def __init__(self, selector: str, unique_ids: List[str], fal_dbt: FalDbt):
        self.raw = selector
        self.children_with_parents = OP_CHILDREN_WITH_PARENTS.match(selector)
        selector = OP_CHILDREN_WITH_PARENTS.rest(selector)

        self.parents = OP_PARENTS.match(selector)
        self.parents_levels = OP_PARENTS.depth(selector)
        selector = OP_PARENTS.rest(selector)

        self.children = OP_CHILDREN.match(selector)
        self.children_levels = OP_CHILDREN.depth(selector)
        selector = OP_CHILDREN.rest(selector)

        self.type = _to_select_type(selector)

        if self.type == SelectType.MODEL:
            self.unique_ids = [f"model.{fal_dbt.project_name}.{selector}"]
        elif self.type == SelectType.SCRIPT:
            self.unique_ids = _expand_script(selector, unique_ids)
        elif self.type == SelectType.COMPLEX:
            self.unique_ids = unique_ids_from_complex_selector(selector, fal_dbt)

    def __post_init__(self):
        if self.children and self.children_with_parents:
            raise RuntimeError(
                f'Invalid node spec {self.raw} - "@" prefix and "+" suffix are incompatible'
            )

    def execute(self, nodeGraph: NodeGraph) -> Iterator[str]:
        for id in self.unique_ids:
            yield id

            if self.children:
                if self.children_levels is None:
                    children = nodeGraph.get_descendants(id)
                else:
                    children = nodeGraph.get_successors(id, self.children_levels)
                yield from children

            if self.parents:
                if self.parents_levels is None:
                    parents = nodeGraph.get_ancestors(id)
                else:
                    parents = nodeGraph.get_predecessors(id, self.parents_levels)
                yield from parents

            if self.children_with_parents:
                ids = _get_children_with_parents(id, nodeGraph)
                yield from ids


def unique_ids_from_complex_selector(select, fal_dbt: FalDbt) -> List[str]:
    args = CompileArgs(None, [select], [select], tuple(), fal_dbt._state, None)
    compile_task = CompileTask(args, fal_dbt._config)
    compile_task._runtime_initialize()
    spec = compile_task.get_selection_spec()
    graph = compile_task.get_node_selector().get_graph_queue(spec)
    return list(graph.queued)


def _to_select_type(selector: str) -> SelectType:
    if ":" in selector:
        return SelectType.COMPLEX
    else:
        if _is_script_node(selector):
            return SelectType.SCRIPT
        else:
            return SelectType.MODEL


def _is_script_node(node_name: str) -> bool:
    return node_name.endswith(".py") or node_name.endswith(".ipynb")


class SelectorGraphOp:
    _regex: re.Pattern

    def __init__(self, regex: re.Pattern):
        self._regex = regex
        assert (
            "rest" in regex.groupindex
        ), 'rest must be in regex. Use `re.compile("something(?P<rest>.*)")`'

    def _select(self, selector: str, group: Union[str, int]) -> Optional[str]:
        match = self._regex.match(selector)
        if match:
            return match.group(group)

    def match(self, selector: str) -> bool:
        return self._select(selector, 0) is not None

    def rest(self, selector: str) -> str:
        rest = self._select(selector, "rest")
        if rest is not None:
            return rest
        return selector


class SelectorGraphOpDepth(SelectorGraphOp):
    def depth(self, selector: str) -> Optional[int]:
        depth = self._select(selector, "depth")
        if depth:
            return int(depth)


# Graph operators from their regex Patterns
OP_CHILDREN_WITH_PARENTS = SelectorGraphOp(re.compile("^\\@(?P<rest>.*)"))
OP_PARENTS = SelectorGraphOpDepth(re.compile("^(?P<depth>\\d*)\\+(?P<rest>.*)"))
OP_CHILDREN = SelectorGraphOpDepth(re.compile("(?P<rest>.*)\\+(?P<depth>\\d*)$"))

# Logic based set operators
OP_SET_UNION = " "
OP_SET_INTERSECTION = ","

IS_BEFORE_SCRIPT_REGEX = re.compile("^script\\..+\\.BEFORE\\..+\\.(py|ipynb)$")
IS_AFTER_SCRIPT_REGEX = re.compile("^script\\..+\\.AFTER\\..+\\.(py|ipynb)")


def _is_before_script(id: str) -> bool:
    return bool(IS_BEFORE_SCRIPT_REGEX.match(id))


def _is_after_script(id: str) -> bool:
    return bool(IS_AFTER_SCRIPT_REGEX.match(id))
