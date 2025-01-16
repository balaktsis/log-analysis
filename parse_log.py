from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id, lag, split, length, startswith
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import sys
import re


def normalize_query(query: str) -> str:
    # Replace parameter values (strings in quotes, numbers) with placeholders
    query = re.sub(r"'.*?'", "?", query.split("QUERY")[1].replace(":",""))  # Replace strings
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


    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KTEO log experiments") \
        .getOrCreate()

    log_file = sys.argv[1] if len(sys.argv) > 1 else "input/audit/20241210.log"
    log_lines = spark.read.text(log_file).toDF("line") \
        .filter((length(col("line")) > 0) & (col("line").startswith("["))) \
        .withColumn("line", col("line").cast("string")) \
        # .withColumn("line", split(col("line"), " ").getItem(2))
        # .show()


    # Trying to check if there is a specific "first query" for each new case
    # first_queries_by_case(log_lines)

    cases, unique_cases = count_unique_cases(log_lines)

    # Stop the Spark session
    spark.stop()

