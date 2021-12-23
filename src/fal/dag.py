from dataclasses import dataclass, field
from typing import List, TypeVar, Dict
from faldbt.project import DbtModel, FalDbt
from pathlib import Path
import re

T = TypeVar("T", bound="FalScript")


class FalDagCycle(Exception):
    pass


@dataclass(frozen=True)
class FalScript:
    model: DbtModel
    path: str
    dependencies: List[T] = field(default_factory=list)

    def __hash__(self):
        return self.path.__hash__() * self.model.name.__hash__()

    def __repr__(self):
        return "FalScript(" + self.path.__str__() + ")"

    def parse(self) -> List[str]:
        """
        Parses the python file to identify any dependencies
        """
        with open(self.path) as file:
            lines = file.readlines()
            ## TODO: do this search in parallel
            dependency_names = []
            for line in lines:
                match = re.search('ref\("(.*)"\)', line)
                if match:
                    group = match.group(1)
                    dependency_names.append(group)
            return dependency_names

    def add_dependency(self, dependency: T):
        if dependency not in self.dependencies and dependency != self:
            self.dependencies.append(dependency)

    def exec(self, context, faldbt: FalDbt):
        """
        Executes the script
        """
        with open(self.path) as file:
            a_script = file.read()
            exec(
                a_script,
                {
                    "context": context,
                    "ref": faldbt.ref,
                    "source": faldbt.source,
                    "write_to_source": faldbt.write_to_source,
                    "write_to_firestore": faldbt.write_to_firestore,
                    "list_models": faldbt.list_models,
                    "list_models_ids": faldbt.list_models_ids,
                    "list_sources": faldbt.list_sources,
                    "list_features": faldbt.list_features
                },
            )


@dataclass(frozen=True)
class _ScriptUniqueKey:
    model: DbtModel
    script_path: Path


class _ScriptGraphBuilder:
    modelNameToModelLookup: Dict[str, DbtModel] = {}
    modelToScriptLookup: Dict[str, List[Path]] = {}
    falScripts: Dict[_ScriptUniqueKey, FalScript] = {}

    def __init__(self, models: List[DbtModel], keyword: str, project_dir: str):
        for model in models:
            self.modelToScriptLookup[model.name] = model.get_script_paths(
                keyword, project_dir
            )
            self.modelNameToModelLookup[model.name] = model
        for model_name, script_list in self.modelToScriptLookup.items():
            for script in script_list:
                self.recursively_set_dependencies(
                    _ScriptUniqueKey(self.modelNameToModelLookup[model_name], script)
                )

    def recursively_set_dependencies(self, key: _ScriptUniqueKey):
        if key in self.falScripts:
            return
        self.falScripts[key] = FalScript(key.model, key.script_path)
        current_script = self.falScripts.get(key)
        dependency_models = current_script.parse()

        for model_name in dependency_models:
            dependency_model = self.modelNameToModelLookup[model_name]
            dependency_scripts = self.modelToScriptLookup.get(model_name, [])

            for dependency_script in dependency_scripts:
                dependency_key = _ScriptUniqueKey(dependency_model, dependency_script)
                self.recursively_set_dependencies(dependency_key)
                current_script.add_dependency(self.falScripts.get(dependency_key))

    def get_values(self) -> List[FalScript]:
        return self.falScripts.values()


class ScriptGraph:
    def __init__(
        self,
        models: List[DbtModel],
        keyword: str,
        project_dir: str,
        _graph: List[FalScript] = [],
    ):
        self.outgoing: Dict[FalScript, List[FalScript]] = {}
        self.incoming: Dict[FalScript, List[FalScript]] = {}
        self.ordered_list: List[FalScript] = []

        if _graph:
            self.graph = _graph
        else:
            self.graph = _ScriptGraphBuilder(models, keyword, project_dir).get_values()
        self.incoming = dict(
            map(lambda script: [script, script.dependencies], self.graph)
        )
        for edge in self.graph:
            self.outgoing[edge] = []
        for edge in self.graph:
            for dependency in edge.dependencies:
                self.outgoing[dependency].append(edge)

    def sort(self):
        """
        Topologically sorts the python scripts to make sure all dependencies
        run before the script itsef
        """
        leaf_nodes = list(
            filter(lambda key: len(self.incoming[key]) == 0, self.incoming.keys())
        )
        while leaf_nodes:
            node = leaf_nodes.pop(0)
            self.ordered_list.append(node)
            outgoing_copy = self.outgoing[node].copy()
            for item in outgoing_copy:
                self.incoming[item].remove(node)
                self.outgoing[node].remove(item)
                if not self.incoming[item]:
                    leaf_nodes.append(item)
        detect_cycles = _flatten(self.outgoing.values())
        if detect_cycles:
            raise FalDagCycle(
                "Your python scripts contain a cycle could not determine the right order"
            )
        return self.ordered_list


def _flatten(forest):
    return [leaf for tree in forest for leaf in tree]
