import networkx as nx

graph = nx.DiGraph(directed=True)

graph.add_nodes_from(
    [
        ("A", {"kind": "dbt model", "post-hook": ["SA1", "SA2", "SA3"]}),
        ("B", {"kind": "dbt model", "post-hook": ["SB1"]}),
        ("B1", {"kind": "dbt model"}),
        ("B2", {"kind": "dbt model"}),
        ("C", {"kind": "dbt model"}),
        ("D", {"kind": "dbt model", "post-hook": ["SD1"]}),
        ("E", {"kind": "dbt model"}),
        ("F", {"kind": "dbt model"}),
        ("G", {"kind": "dbt model"}),
        ("J", {"kind": "dbt model"}),
        ("K", {"kind": "dbt model", "post-hook": ["SK1"]}),
        ("L", {"kind": "dbt model", "post-hook": ["SL1"]}),
        ("H", {"kind": "python model"}),
    ]
)

graph.add_edges_from(
    [
        ("A", "B"),
        ("A", "B1"),
        ("A", "B2"),
        ("B", "L"),
        ("B1", "D"),
        ("B2", "D"),
        ("C", "D"),
        ("D", "E"),
        ("E", "F"),
        ("F", "G"),
        ("G", "H"),
        ("G", "J"),
        ("H", "K"),
        ("J", "K"),
        ("K", "L"),
    ]
)
