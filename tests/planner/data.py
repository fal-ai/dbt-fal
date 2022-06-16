from tests.planner.utils import to_graph
from fal.node_graph import NodeKind

GRAPH_1 = [
    ("A", {"kind": NodeKind.DBT_MODEL, "post_hook": ["S1", "S2"], "to": ["B"]}),
    ("B", {"kind": NodeKind.DBT_MODEL, "post_hook": ["S3", "S4"], "to": ["E"]}),
    ("C", {"kind": NodeKind.DBT_MODEL, "to": ["D"]}),
    ("D", {"kind": NodeKind.DBT_MODEL, "to": ["E"]}),
    ("E", {"kind": NodeKind.FAL_MODEL, "post_hook": ["S5"], "to": ["F"]}),
    ("F", {"kind": NodeKind.DBT_MODEL}),
]

GRAPH_2 = [
    (
        "A",
        {
            "kind": NodeKind.DBT_MODEL,
            "post_hook": ["SA1", "SA2", "SA3"],
            "to": ["B", "B1", "B2"],
        },
    ),
    ("B", {"kind": NodeKind.DBT_MODEL, "post_hook": ["SB1"], "to": ["L"]}),
    ("B1", {"kind": NodeKind.DBT_MODEL, "to": ["D"]}),
    ("B2", {"kind": NodeKind.DBT_MODEL, "to": ["D"]}),
    ("C", {"kind": NodeKind.DBT_MODEL, "to": ["D"]}),
    ("D", {"kind": NodeKind.DBT_MODEL, "post_hook": ["SD1"], "to": ["E"]}),
    ("E", {"kind": NodeKind.DBT_MODEL, "to": ["F"]}),
    ("F", {"kind": NodeKind.DBT_MODEL, "to": ["G"]}),
    ("G", {"kind": NodeKind.DBT_MODEL, "to": ["H", "J"]}),
    ("H", {"kind": NodeKind.FAL_MODEL, "to": ["K"]}),
    ("J", {"kind": NodeKind.DBT_MODEL, "to": ["K"]}),
    ("K", {"kind": NodeKind.DBT_MODEL, "post_hook": ["SK1"], "to": ["L"]}),
    ("L", {"kind": NodeKind.DBT_MODEL, "post_hook": ["SL1"]}),
]


GRAPHS = [
    {"graph": to_graph(GRAPH_1), "subgraphs": set()},
    {
        "graph": to_graph(GRAPH_2),
        "subgraphs": {frozenset(("B1", "B2")), frozenset(("E", "F", "G"))},
    },
]
