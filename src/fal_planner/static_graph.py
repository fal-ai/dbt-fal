import networkx as nx

VARIABLE_TIMES = False
graph = nx.DiGraph(directed=True)

if VARIABLE_TIMES:
    graph.add_nodes_from(
        [
            ("A", {"kind": "dbt model", "time": 1}),
            ("B", {"kind": "dbt model", "time": 2}),
            ("C", {"kind": "dbt model", "time": 5}),
            ("D", {"kind": "dbt model", "time": 1}),
            ("F", {"kind": "dbt model", "time": 1}),
            ("E", {"kind": "python model", "time": 2}),
            ("S1", {"kind": "python script", "time": 1}),
            ("S2", {"kind": "python script", "time": 2}),
            ("S3", {"kind": "python script", "time": 2}),
            ("S4", {"kind": "python script", "time": 3}),
            ("S5", {"kind": "python script", "time": 4}),
        ]
    )
else:
    graph.add_nodes_from(
        [
            ("A", {"kind": "dbt model", "time": 2}),
            ("B", {"kind": "dbt model", "time": 2}),
            ("C", {"kind": "dbt model", "time": 2}),
            ("D", {"kind": "dbt model", "time": 2}),
            ("F", {"kind": "dbt model", "time": 2}),
            ("E", {"kind": "python model", "time": 2}),
            ("S1", {"kind": "python script", "time": 2}),
            ("S2", {"kind": "python script", "time": 2}),
            ("S3", {"kind": "python script", "time": 2}),
            ("S4", {"kind": "python script", "time": 2}),
            ("S5", {"kind": "python script", "time": 2}),
        ]
    )

graph.add_edges_from(
    [
        ("A", "S1"),
        ("A", "S2"),
        ("S1", "B"),
        ("S2", "B"),
        ("B", "S3"),
        ("B", "S4"),
        ("S3", "E"),
        ("S4", "E"),
        ("C", "D"),
        ("D", "E"),
        ("E", "F"),
        ("S5", "F"),
    ]
)
