import json
import networkx as nx
from pyvis.network import Network
import textwrap
import matplotlib.colors as mcolors
from datetime import datetime
import re

JSON_FILE = "output/" + "grouped_logs_by_arith_kykl.json"

def get_color_for_confidence(confidence):
    cmap = mcolors.LinearSegmentedColormap.from_list("conf_cmap", ["red", "blue", "green"])
    norm = mcolors.Normalize(vmin=0, vmax=1)
    rgba = cmap(norm(confidence))
    return mcolors.to_hex(rgba)

def parse_json(json_file):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    query_chains = {}
    query_to_node = {}  # Maps (query, arith_kykl) to unique node ID
    
    node_counter = 0
    for entry in data:
        key = entry["ARITH_KYKL"]
        queries = entry["grouped_lines"]
        
        sorted_queries = sorted(queries, key=lambda x: datetime.fromisoformat(x["timestamp"]))
        
        query_chains[key] = []
        for q in sorted_queries:
            query_text = q["normalized_line"].split("QUERY      : ")[1]
            query_identifier = (query_text, key)  # Query text and ARITH_KYKL together as a unique identifier
            
            if query_identifier not in query_to_node:
                query_to_node[query_identifier] = node_counter
                node_counter += 1
            
            query_chains[key].append((query_to_node[query_identifier], q["timestamp"]))  # Store node ID and timestamp
    
    return query_chains, query_to_node

def build_query_graph(query_chains):
    G = nx.DiGraph()
    
    # Add nodes and edges
    for key, queries in query_chains.items():
        for i in range(len(queries) - 1):
            start_node = queries[i][0]
            end_node = queries[i + 1][0]
            timestamp = queries[i + 1][1]
            G.add_edge(start_node, end_node, label=str(timestamp).split("T")[1].split(".")[0]) 
    
    return G

def plot_query_graph(G, query_to_node):
    net = Network(notebook=True, height="800px", width="100%", directed=True)
    
    # Add nodes with the query label on hover
    for node in G.nodes():
        query_text = list(query_to_node.keys())[list(query_to_node.values()).index(node)]
        net.add_node(node, title=query_text, color="lightblue", size=20)
    
    # Add edges with timestamp as label
    for edge in G.edges(data=True):
        start, end, data = edge
        net.add_edge(start, end, label=data["label"], color="black")
    
    net.show("output/query_chains_graph.html")

if __name__ == "__main__":
    query_chains, query_to_node = parse_json(JSON_FILE)
    G = build_query_graph(query_chains)
    plot_query_graph(G, query_to_node)
