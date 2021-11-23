from dataclasses import dataclass
from typing import List
from fal.dag import FalDagCycle, FalScript, ScriptGraph
from faldbt.project import DbtModel
from dataclasses import dataclass
import pytest


@dataclass
class MockDbtModel:
    name: str


def fal_scripts() -> List[FalScript]:
    scripts = []
    for letter in ["A", "B", "C", "D"]:
        scripts.append(FalScript(MockDbtModel(letter), f"./{letter}.py"))
    return scripts


def test_simple_dag():
    [scriptA, scriptB, scriptC, _] = fal_scripts()

    graph = ScriptGraph(
        models=[],
        keyword="test",
        project_dir="test",
        _graph=[scriptA, scriptB, scriptC],
    )
    sorted = graph.sort()
    assert sorted == [scriptA, scriptB, scriptC]


def test_dag_with_dependencies():

    [scriptA, scriptB, scriptC, scriptD] = fal_scripts()
    scriptB.add_dependency(scriptA)
    scriptC.add_dependency(scriptB)

    script_graph = ScriptGraph(
        models=[],
        keyword="test",
        project_dir="test",
        _graph=[scriptA, scriptB, scriptC, scriptD],
    )
    sorted = script_graph.sort()
    assert sorted == [scriptA, scriptD, scriptB, scriptC]


def test_with_cycle():
    [scriptA, scriptB, scriptC, _] = fal_scripts()
    scriptA.add_dependency(scriptB)
    scriptB.add_dependency(scriptC)
    scriptC.add_dependency(scriptA)

    script_graph = ScriptGraph(
        models=[],
        keyword="test",
        project_dir="test",
        _graph=[scriptA, scriptB, scriptC],
    )
    with pytest.raises(FalDagCycle):
        script_graph.sort()
