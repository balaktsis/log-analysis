import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from collections import defaultdict

def build_query_chains(confidence_df, min_chain_confidence=0.01, max_chain_length=5):
    query_graph = defaultdict(list)
    confidence_map = {}

    unique_queries = confidence_df.select("normalized_query").rdd.flatMap(lambda x: x).collect()
    unique_queries += confidence_df.select("next_query").rdd.flatMap(lambda x: x).collect()
    unique_queries = sorted(set(unique_queries))  # Remove duplicates

    # Assign unique IDs
    query_to_id = {query: f"Q{i+1}" for i, query in enumerate(unique_queries)}

    legend_df = pd.DataFrame(query_to_id.items(), columns=["Query", "ID"])
    legend_df.to_csv("output/query_legend.csv", index=False)

    for row in confidence_df.collect():
        start, end, conf = row["normalized_query"], row["next_query"], row["confidence"]

        start_id, end_id = query_to_id[start], query_to_id[end]
        query_graph[start_id].append(end_id)
        confidence_map[(start_id, end_id)] = conf  

    chains = []

    def dfs(chain, current_confidence, visited):
        if len(chain) > max_chain_length:
            return
        last_query = chain[-1]
        if last_query not in query_graph:
            return  
        for next_query in query_graph[last_query]:
            if next_query in visited:  
                continue
            new_conf = current_confidence * confidence_map[(last_query, next_query)]
            new_chain = chain + [next_query]
            visited.add(next_query) 
            if new_conf >= min_chain_confidence:
                chains.append((new_chain, new_conf))
                dfs(new_chain, new_conf, visited)  
            visited.remove(next_query) 

    for start_query in query_graph.keys():
        visited = set([start_query])  
        dfs([start_query], 1.0, visited)

    return sorted(chains, key=lambda x: -x[1]), confidence_map



def plot_query_chains(chains, confidence_map):
    G = nx.DiGraph()

    for chain, conf in chains:
        for i in range(len(chain) - 1):
            if (chain[i], chain[i + 1]) in confidence_map:
                G.add_edge(chain[i], chain[i + 1], confidence=confidence_map[(chain[i], chain[i + 1])])

    # node_colors = []
    # for node in G.nodes():
    #     if node in fisrt_query:
    #         node_colors.append("lightgreen")  # Special color for nodes in the list
    #     else:
    #         node_colors.append("lightblue")  # Default color for other nodes


    pos = nx.spring_layout(G, k=0.2)  
    plt.figure(figsize=(12, 8))

    edges = G.edges(data=True)
    edge_labels = {(u, v): f"{d['confidence']:.2f}" for u, v, d in edges}

    nx.draw(G, pos, with_labels=True, node_color="lightblue", edge_color="gray", node_size=400, font_size=8)
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=7)

    plt.title("Query Chains with Confidence Scores")
    plt.savefig("output/query_chains_graph.png",dpi=300)
    plt.show()


spark = SparkSession.builder.getOrCreate()
confidence_df = spark.read.csv("output/ins-upd_consec_pairs.csv", header=True, inferSchema=True).withColumn("confidence", col("confidence").cast("double"))

chains, confidence_map = build_query_chains(confidence_df, min_chain_confidence=0.8, max_chain_length=1)
plot_query_chains(chains, confidence_map)
