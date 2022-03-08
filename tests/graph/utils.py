from fal import DbtModel

def assert_contains_all(thisList, otherList):
    for other in otherList:
        assert other in thisList


def create_mock_model(model_node_class, name, script_paths, depends_on_nodes=[]):
    parsedNodeMockInstance = model_node_class()
    parsedNodeMockInstance.name = name
    parsedNodeMockInstance.depends_on_nodes = depends_on_nodes
    model = DbtModel(parsedNodeMockInstance)
    model.unique_id = "model." + name
    model.get_script_paths = MagicMock(return_value=script_paths)
    return model
