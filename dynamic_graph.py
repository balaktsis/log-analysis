import networkx as nx
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from collections import defaultdict
from pyvis.network import Network
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import textwrap
import matplotlib.colors as mcolors



DATASET_CSV = "output/" + "mixed_consec_pairs.csv"
CONF = 0.5



def get_color_for_confidence(confidence):
    """
    Maps confidence values to a color gradient from red (low confidence) to green (high confidence).
    """
    cmap = mcolors.LinearSegmentedColormap.from_list("conf_cmap", ["red", "blue", "green"])
    norm = mcolors.Normalize(vmin=0, vmax=1) 
    rgba = cmap(norm(confidence))
    return mcolors.to_hex(rgba)


def build_query_chains(confidence_df, min_chain_confidence=0.01, max_chain_length=5):
    query_graph = defaultdict(list)
    confidence_map = {}

    unique_queries = confidence_df.select("normalized_query").rdd.flatMap(lambda x: x).collect()
    unique_queries += confidence_df.select("next_query").rdd.flatMap(lambda x: x).collect()
    unique_queries = sorted(set(unique_queries))  # Remove duplicates

    # Assign unique IDs
    query_to_id = {query: f"Q{i+1}" for i, query in enumerate(unique_queries)}

    legend_df = pd.DataFrame(query_to_id.items(), columns=["Query", "ID"])
    legend_df.to_csv("output/query_legend_" +DATASET_CSV.split("/")[1].split(".")[0] +".csv", index=False)

    for row in confidence_df.collect():
        start, end, conf, count, total = row["normalized_query"], row["next_query"], row["confidence"], row["count"], row["total_count"]

        start_id, end_id = query_to_id[start], query_to_id[end]
        query_graph[start_id].append(end_id)
        confidence_map[(start_id, end_id)] = [conf, count, total]  

    chains = []

    def dfs(chain, current_confidence, visited):
        if len(chain) > max_chain_length:
            return
        last_query = chain[-1]
        if last_query not in query_graph:
            return  
        for next_query in query_graph[last_query]:
            # if next_query in visited:  
            #     continue
            new_conf = current_confidence * confidence_map[(last_query, next_query)][0]
            new_chain = chain + [next_query]
            visited.add(next_query) 
            if new_conf >= min_chain_confidence:
                chains.append((new_chain, new_conf))
                dfs(new_chain, new_conf, visited)  
            visited.remove(next_query) 

    for start_query in query_graph.keys():
        visited = set([start_query])  
        dfs([start_query], 1.0, visited)

    return query_to_id, sorted(chains, key=lambda x: -x[1]), confidence_map

import json

def get_connected_components(G, query_map):
    G_undirected = G.to_undirected()

    # Find all connected components in the undirected graph
    components = list(nx.connected_components(G_undirected))
    connected_components_dict = {}
    
    # Create a dictionary for each connected component
    for component_id, component in enumerate(components, start=1):
        queries_in_component = [[key for key, val in query_map.items() if val == node][0] for node in component]
        connected_components_dict[component_id] = queries_in_component
    
    return connected_components_dict

def plot_query_chains(chains, confidence_map, query_map: dict):
    G = nx.DiGraph()
    G.name = "Query Chains"

    for chain, conf in chains:
        for i in range(len(chain) - 1):
            if (chain[i], chain[i + 1]) in confidence_map:
                G.add_edge(chain[i], chain[i + 1], confidence=confidence_map[(chain[i], chain[i + 1])][0], count=confidence_map[(chain[i], chain[i + 1])][1], total=confidence_map[(chain[i], chain[i + 1])][2])

    net = Network(notebook=True, height="800px", width="100%", directed=True)

    for node in G.nodes():
        query_info = [key for key, val in query_map.items() if val == node][0]     
        formatted_query_info = "\n".join(textwrap.wrap(query_info, width=60))  
        color = "black"
        if "SELECT" in query_info:
            color = "blue"
        elif "INSERT" in query_info:
            color = "green"
        else:
            color = "red"
        net.add_node(
            node, 
            label=node, 
            color=color,
            title=formatted_query_info,
        )
    
    for edge in G.edges(data=True):
        start, end, data = edge
        confidence = data['confidence']
        edge_color = get_color_for_confidence(confidence)
        net.add_edge(
            start, 
            end, 
            label=f"{confidence:.2f}",  # + "; " + str(data["count"]) + "/" + str(data["total"]),           
            color=edge_color                       

        )

    # Get connected components and save as JSON
    connected_components_dict = get_connected_components(G, query_map)
    
    # Save the connected components dictionary as a JSON file
    json_filename = "output/connected_components_" + DATASET_CSV.split("/")[1].split(".")[0] + ".json"
    with open(json_filename, 'w') as json_file:
        json.dump(connected_components_dict, json_file, indent=4)

    net.toggle_physics(True)

    net.show("output/query_chains_graph_" + DATASET_CSV.split("/")[1].split(".")[0] + ".html")



spark = SparkSession.builder.getOrCreate()
confidence_df = spark.read.csv(DATASET_CSV, header=True, inferSchema=True).withColumn("confidence", col("confidence").cast("double"))\
                                                                            .withColumn("total_count", col("total_count").cast("int"))\
                                                                            .withColumn("count", col("count").cast("int"))                         

query_map, chains, confidence_map = build_query_chains(confidence_df, min_chain_confidence=CONF, max_chain_length=1)
plot_query_chains(chains, confidence_map, query_map)