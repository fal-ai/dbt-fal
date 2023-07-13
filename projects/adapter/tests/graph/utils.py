from fal.dbt import DbtModel
from copy import deepcopy
from unittest.mock import MagicMock


def assert_contains_only(thisList, otherList):
    assert len(thisList) == len(otherList)
    for other in otherList:
        assert other in thisList


def create_mock_model(
    parsedNodeMockInstance,
    name,
    script_paths,
    depends_on_models,
    before_script_paths=[],
) -> DbtModel:
    node = deepcopy(parsedNodeMockInstance)
    node.unique_id = "model." + name
    node.name = name
    model = DbtModel(node)

    def script_calculations(before: bool = False):
        if before:
            return before_script_paths
        else:
            return script_paths

    model.get_scripts = MagicMock(side_effect=script_calculations)
    model.get_depends_on_nodes = MagicMock(return_value=depends_on_models)
    return model
