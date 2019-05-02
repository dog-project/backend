from collections import defaultdict

from util.get_pool import get_connection
from xranking import get_victory_graph
import networkx as nx
import json
import pygraphviz as pgv

with open('../../credentials.json') as creds:
    conn = get_connection(json.load(creds))

matchups = get_victory_graph(conn, defaultdict(bool))

win_edges = sorted([edge for edge in matchups.edges],
                   key=lambda x: matchups.get_edge_data(x[0], x[1])["margin"],
                   reverse=True)

print(win_edges)
new_graph = nx.DiGraph()
new_graph.add_nodes_from(matchups.nodes)
for (u, v) in win_edges:
    if v not in nx.descendants(new_graph, u):
        new_graph.add_edge(u, v)
        print(f"adding {u}, {v}")

to_remove = []
e = [x for x in new_graph.edges]
for (u, v) in e:
    new_graph.remove_edge(u, v)
    has_other_path = nx.has_path(new_graph, u, v)

    # print(u, v, list(paths), len(list(paths)))
    if not has_other_path:
        print(f"keeping {u}, {v}")
        new_graph.add_edge(u, v)

new_graph.remove_edges_from(to_remove)

G = pgv.AGraph(directed=True)
G.add_nodes_from(matchups.nodes)
G.node_attr['shape'] = 'square'
G.add_edges_from(new_graph.edges)

G.layout(prog='dot')
G.draw('file.png')
