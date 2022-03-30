from more_itertools import side_effect
from sqlalchemy import false, true
from fal import DbtModel
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
    model = DbtModel(parsedNodeMockInstance)
    model.unique_id = "model." + name

    def script_calculations(keyword: str, project_dir: str, before: bool = False):
        if before:
            return before_script_paths
        else:
            return script_paths

    model.get_script_paths = MagicMock(side_effect=script_calculations)
    model.name = name
    model.get_depends_on_nodes = MagicMock(return_value=depends_on_models)
    return model
