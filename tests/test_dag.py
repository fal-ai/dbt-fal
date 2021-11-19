from dataclasses import dataclass
from fal.dag import FalDagCycle, FalScript, ScriptGraph
from faldbt.project import DbtModel
from dataclasses import dataclass
import pytest

@dataclass
class MockDbtModel:
    name: str

def test_simple_dag():
    scriptA = FalScript(MockDbtModel("A"), "/scriptA", dependencies=[])
    scriptB = FalScript(MockDbtModel("B"), "/scriptB", dependencies=[])
    scriptC = FalScript(MockDbtModel("C"), "/scriptC", dependencies=[])

    graph = ScriptGraph(models=[], keyword= "test", root="test", graph=[scriptA, scriptB, scriptC])
    sorted = graph.sort()
    assert sorted == [scriptA, scriptB, scriptC]


def test_dag_with_dependencies():
    
    scriptA = FalScript(MockDbtModel("A"), "/scriptA", dependencies=[])
    scriptB = FalScript(MockDbtModel("B"), "/scriptB", dependencies=[scriptA])
    scriptC = FalScript(MockDbtModel("C"), "/scriptC", dependencies=[scriptB])
    scriptD = FalScript(MockDbtModel("D"), "/scriptD", dependencies=[])

    script_graph = ScriptGraph(models=[], keyword= "test", root="test", graph=[scriptA, scriptB, scriptC, scriptD])
    sorted = script_graph.sort()
    assert sorted == [scriptA, scriptD, scriptB, scriptC]

def test_with_cycle():
    scriptC = FalScript(MockDbtModel("C"), "/scriptC", dependencies=[])
    scriptA = FalScript(MockDbtModel("A"), "/scriptA", dependencies=[])
    scriptB = FalScript(MockDbtModel("B"), "/scriptB", dependencies=[scriptC])
    scriptC.add_dependency(scriptB)

    script_graph = ScriptGraph(models=[], keyword= "test", root="test", graph=[scriptA, scriptB, scriptC])
    with pytest.raises(FalDagCycle):
        script_graph.sort()
