import networkx as nx

VARIABLE_TIMES = False
graph = nx.DiGraph(directed=True)

graph.add_nodes_from(
    [
        ("A", {"kind": "dbt model", "post-hook": ["S1", "S2"]}),
        ("B", {"kind": "dbt model", "post-hook": ["S3", "S4"]}),
        ("C", {"kind": "dbt model"}),
        ("D", {"kind": "dbt model"}),
        ("F", {"kind": "dbt model"}),
        ("E", {"kind": "python model", "post-hook": ["S5"]}),
    ]
)

graph.add_edges_from(
    [
        ("A", "B"),
        ("B", "E"),
        ("C", "D"),
        ("D", "E"),
        ("E", "F"),
    ]
)
