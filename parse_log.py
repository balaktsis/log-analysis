from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id, lag, split, length, startswith, row_number, lead
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, StructType
import json
import sys
import re


def normalize_query(query: str) -> str:
    # Replace parameter values (strings in quotes, numbers) with placeholders
    if "QUERY" in query:
        query = re.sub(r"'.*?'", "?", query.split("QUERY")[1].replace(":",""))
    elif "INSERT INTO" in query:
        query = "INSERT INTO " + re.sub(r"'.*?'", "?", query.split("INSERT INTO")[1].replace(":",""))
    else:
        query = "UPDATE " + re.sub(r"'.*?'", "?", query.split("UPDATE")[1].replace(":",""))  
    query = re.sub(r"\b\d+(\.\d+)?\b", "?", query)  # Replace numbers
    query = re.sub(r"\s+", " ", query).strip()  # Normalize whitespace


    return query

normalize_query_udf = udf(normalize_query, StringType())



# Find the first queries by case ID and count how many times each query appeared
def first_queries_by_case(log_lines):
    # Filter only QUERY lines and extract Case ID and normalize queries
    queries = log_lines.filter(col("line").contains("QUERY")) \
        .withColumn("case_id", split(col("line"), " ").getItem(2))  \
        .withColumn("normalized_query", normalize_query_udf(col("line"))) \
        .withColumn("row_id", monotonically_increasing_id()) 

    queries.union(log_lines.filter(col("line").contains("TRANSACTION")) \
        .withColumn("case_id", split(col("line"), " ").getItem(2))  \
        .withColumn("normalized_query", normalize_query_udf(col("line"))) \
        .withColumn("row_id", monotonically_increasing_id())) 

    window_spec = Window.partitionBy("case_id").orderBy("row_id")

    # Find the first query per case_id (the first row for each case_id)
    queries_with_first_query = queries.withColumn("is_first_query", (lag("normalized_query", 1).over(window_spec).isNull()).cast("int"))

    # Group by case_id and first_query, and count how many times the first query appeared
    first_query_counts = queries_with_first_query.filter(col("is_first_query") == 1) \
        .groupBy("case_id", "normalized_query") \
        .count()

    sorted_first_query_counts = first_query_counts.orderBy(col("count").desc())

    output_file = "output/first_queries_by_case"
    sorted_first_query_counts.write.csv(output_file, header=True, mode="overwrite")


def count_unique_cases(log_lines):
    cases = log_lines.withColumn("case_id", split(col("line"), " ").getItem(2)).select("case_id") \
        .filter(col("case_id").rlike("^[0-9]+$"))\
        .distinct()
    cases.show()
    print(cases.count())
    return cases, cases.count()


def count_unique_queries(log_lines):
    queries = log_lines.filter(col("line").contains("QUERY") | col("line").contains("INSERT INTO") | col("line").contains("UPDATE")) \
        .withColumn("normalized_query", normalize_query_udf(col("line")))
    
    # queries.union(log_lines.filter(col("line").contains("TRANSACTION")) \
    #     .withColumn("normalized_query", normalize_query_udf(col("line"))))
    
    # Group by normalized query and count occurrences
    query_groups = queries.groupBy("normalized_query").count()

    # Sort results by count in descending order
    sorted_query_groups = query_groups.orderBy(col("count").desc())

    # Save results to a file
    output_file = "output/normalized_queries"
    sorted_query_groups.write.csv(output_file, header=True, mode="overwrite")

    return sorted_query_groups



def extract_consecutive_pairs(log_lines,valid_queries, threshold=300):
    spark = SparkSession.builder.getOrCreate()
    bValid = spark.sparkContext.broadcast(set(valid_queries))
    # Extract queries with row id in order to create the pairs latter


    queries = log_lines.filter(col("line").contains("QUERY") | col("line").contains("INSERT INTO") | col("line").contains("UPDATE")) \
        .withColumn("normalized_query", normalize_query_udf(col("line"))) \
        .filter(col("normalized_query").isin(bValid.value)) \
        .rdd.zipWithIndex().map(lambda x: (x[0][0], x[0][1], x[1])) \
        .toDF(["line", "normalized_query", "row_id"])

    # Create consecutive pairs, group based on the pair and count them
    # At the end maintains only the pairs that occure more than `threshold` times
    w = Window.orderBy("row_id")
    pairs = queries.withColumn("next_query", lead("normalized_query").over(w))\
        .filter(col("next_query").isNotNull())\
        .select("normalized_query", "next_query")\
        .groupBy("normalized_query", "next_query").count()\
        .filter(col("count") > threshold)\
        .orderBy(col("count").desc())
    
    # Write frequent pairs to a file
    output_file = "output/frequent_query_pairs"
    pairs.write.csv(output_file, header=True, mode="overwrite")




def extract_table_names(log_lines):
    transactions = log_lines.filter(
        (col("line").contains("INSERT INTO")) | (col("line").contains("UPDATE"))
    )
    
    table_regex = r"(INSERT INTO|UPDATE)\s+([a-zA-Z0-9_]+)"
    extracted_tables = transactions.rdd.map(lambda row: re.search(table_regex, row.line)) \
                                        .filter(lambda match: match is not None) \
                                        .map(lambda match: match.group(2)) \
                                        .distinct() \
                                        .collect()
    
    print("Tables found in INSERT/UPDATE transactions:")
    for table in extracted_tables:
        print(table)

    return extracted_tables



def extract_arith_kykl_values(log_lines):
    queries = log_lines.filter(col("line").contains("QUERY"))

    arith_kykl_regex = r"ARITH_KYKL\s*=\s*'(\d+)'"

    arith_kykl_values = (
        queries.rdd
        .flatMap(lambda row: re.findall(arith_kykl_regex, row.line))
        .distinct()  
        .collect()  
    )

    print("Unique ARITH_KYKL values: ", len(arith_kykl_values))
    for value in arith_kykl_values:
        print(value)

    return arith_kykl_values



def group_queries_by_arith_kykl(log_lines):
    # Filter for lines containing SELECT queries
    select_queries = log_lines.filter(col("line").contains("QUERY") & col("line").contains("SELECT"))

    # Regex pattern to extract ARITH_KYKL and the entire query
    arith_kykl_regex = r"ARITH_KYKL\s*=\s*'(\d+)'"

    # Extract ARITH_KYKL values and corresponding queries
    grouped_queries = (
        select_queries.rdd
        .flatMap(lambda row: [
            (arith_kykl, row.line) for arith_kykl in re.findall(arith_kykl_regex, row.line)
        ])
        .groupByKey()
        .mapValues(list)
        .collectAsMap()
    )

    grouped_queries_normalized = {
        arith_kykl: [normalize_query(query) for query in queries]
        for arith_kykl, queries in grouped_queries.items()
    }

    with open("output/grouped_by_arkyk.json", "w+", encoding="utf-8") as f:
        json.dump(grouped_queries_normalized, f, ensure_ascii=False, indent=4)

    return grouped_queries

    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KTEO log experiments") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    log_file = sys.argv[1] if len(sys.argv) > 1 else "input/audit/20241210.log"

    data=[]
    with open(log_file, 'r', encoding='windows-1253', errors='replace') as f:
        for line in f:
            # print(line)           # uncomment to see if encoding is correct
            if  "Ελεγκτης" in line and line[0]=="[":
                data.append(line)

    schema = StructType([StructField("line", StringType(), True)])

    log_lines=spark.createDataFrame([(s,) for s in data], schema=schema)

    # Extract table names from INSERT/UPDATE transactions
    table_names = extract_table_names(log_lines)

    # Count unique queries
    valid_queries = count_unique_queries(log_lines)\
        .filter(col("count")<500)\
        .select("normalized_query")\
        .rdd.flatMap(lambda x: x).collect()

    for table in table_names:
        filter(lambda x: table in x, valid_queries)
    
    # print("Unique queries: ", len(valid_queries))


    # # Extract arith_kykl instances
    # arith_kykl_values = extract_arith_kykl_values(log_lines)

    # # Group queries by ARITH_KYKL
    # grouped_queries = group_queries_by_arith_kykl(log_lines)
   
    # Extract consecutive pairs
    extract_consecutive_pairs(log_lines, valid_queries,50)
    
    # Check if there is a specific "first query" for each new case
    # first_queries_by_case(log_lines)


   
    # cases, unique_cases = count_unique_cases(log_lines)

    # Stop the Spark session
    spark.stop()

