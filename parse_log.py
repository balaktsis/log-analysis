from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id, lag, split, length, startswith, row_number, lead
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import sys
import re


def normalize_query(query: str) -> str:
    # Replace parameter values (strings in quotes, numbers) with placeholders
    if "QUERY" in query:
        query = re.sub(r"'.*?'", "?", query.split("QUERY")[1].replace(":",""))  # Replace strings
    else:
        query = re.sub(r"'.*?'", "?", query.split("TRANSACTION")[1].replace(":",""))  # Replace strings
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
    queries = log_lines.filter(col("line").contains("QUERY")) \
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


    queries = log_lines.filter(col("line").contains("QUERY")) \
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

    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KTEO log experiments") \
        .master("local[6]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    log_file = sys.argv[1] if len(sys.argv) > 1 else "input/audit/20241210.log"

    data=[]
    with open(log_file, 'r', encoding='windows-1253', errors='replace') as f:
        for line in f:
            if line and "Ελεγκτης" in line and line[0]=="[":
                data.append(line)

    log_lines=spark.createDataFrame([(s,) for s in data],["line"])

    # log_lines = spark.read.format("text").option("encoding", "Cp1253").load(log_file).toDF("line") \
    #     .filter((length(col("line")) > 0) & (col("line").startswith("["))) \
    #     .withColumn("line", col("line").cast("string")) \
        # .withColumn("line", split(col("line"), " ").getItem(2))
        # .show()

    # Trying to count unique queries
    valid_queries = count_unique_queries(log_lines)\
        .filter(col("count")<200)\
        .select("normalized_query")\
        .rdd.flatMap(lambda x: x).collect()

    # Extract consecutive pairs
    extract_consecutive_pairs(log_lines, valid_queries,50)
    


    # Trying to check if there is a specific "first query" for each new case
    # first_queries_by_case(log_lines)

    # cases, unique_cases = count_unique_cases(log_lines)

    # Stop the Spark session
    spark.stop()

