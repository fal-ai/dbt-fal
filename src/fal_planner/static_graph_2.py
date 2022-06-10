import networkx as nx

graph = nx.DiGraph(directed=True)

graph.add_nodes_from(
    [
        ("A", {"kind": "dbt model", "time": 2}),
        ("B", {"kind": "dbt model", "time": 2}),
        ("B1", {"kind": "dbt model", "time": 2}),
        ("B2", {"kind": "dbt model", "time": 2}),
        ("C", {"kind": "dbt model", "time": 2}),
        ("D", {"kind": "dbt model", "time": 2}),
        ("F", {"kind": "dbt model", "time": 2}),
        ("E", {"kind": "python model", "time": 2}),
        ("SA1", {"kind": "python script", "time": 2}),
        ("SA2", {"kind": "python script", "time": 2}),
        ("SA3", {"kind": "python script", "time": 2}),
        ("SB1", {"kind": "python script", "time": 2}),
        ("SB2", {"kind": "python script", "time": 2}),
        ("SE1", {"kind": "python script", "time": 2}),
    ]
)

graph.add_edges_from(
    [
        ("A", "SA1"),
        ("A", "SA2"),
        ("A", "SA3"),
        ("SA1", "B"),
        ("SA2", "B"),
        ("SA3", "B"),
        ("SA3", "B1"),
        ("SA3", "B2"),
        ("B", "SB1"),
        ("B", "SB2"),
        ("B1", "D"),
        ("B2", "D"),
        ("SB1", "E"),
        ("SB2", "E"),
        ("C", "D"),
        ("D", "E"),
        ("E", "SE1"),
        ("SE1", "F"),
    ]
)
