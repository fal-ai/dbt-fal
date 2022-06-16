from tests.planner.utils import to_graph

GRAPH_1 = [
    ("A", {"kind": "dbt model", "post-hook": ["S1", "S2"], "to": ["B"]}),
    ("B", {"kind": "dbt model", "post-hook": ["S3", "S4"], "to": ["E"]}),
    ("C", {"kind": "dbt model", "to": ["D"]}),
    ("D", {"kind": "dbt model", "to": ["E"]}),
    ("E", {"kind": "python model", "post-hook": ["S5"], "to": ["F"]}),
    ("F", {"kind": "dbt model"}),
]

GRAPH_2 = [
    (
        "A",
        {
            "kind": "dbt model",
            "post-hook": ["SA1", "SA2", "SA3"],
            "to": ["B", "B1", "B2"],
        },
    ),
    ("B", {"kind": "dbt model", "post-hook": ["SB1"], "to": ["L"]}),
    ("B1", {"kind": "dbt model", "to": ["D"]}),
    ("B2", {"kind": "dbt model", "to": ["D"]}),
    ("C", {"kind": "dbt model", "to": ["D"]}),
    ("D", {"kind": "dbt model", "post-hook": ["SD1"], "to": ["E"]}),
    ("E", {"kind": "dbt model", "to": ["F"]}),
    ("F", {"kind": "dbt model", "to": ["G"]}),
    ("G", {"kind": "dbt model", "to": ["H", "J"]}),
    ("H", {"kind": "python model", "to": ["K"]}),
    ("J", {"kind": "dbt model", "to": ["K"]}),
    ("K", {"kind": "dbt model", "post-hook": ["SK1"], "to": ["L"]}),
    ("L", {"kind": "dbt model", "post-hook": ["SL1"]}),
]


GRAPHS = [
    {"graph": to_graph(GRAPH_1), "subgraphs": set()},
    {
        "graph": to_graph(GRAPH_2),
        "subgraphs": {frozenset(("B1", "B2")), frozenset(("E", "F", "G"))},
    },
]
