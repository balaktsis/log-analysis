"""Module to the structure of SQL queries. It will provide the general functionality to extract the 
   basic statements from each sql query. Possible extension include the extraction of the where conditions.
"""
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Token, Parenthesis, Function, Comparison
from sqlparse.tokens import Keyword, DML, Whitespace
import json
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructField, StructType

def _extract_from_statement(token_list:list)->tuple[list,dict]:
    """Helper function to find table name in tokens."""
    appending = False
    from_tokens = []
    tables = []
    for token_id in range(len(token_list)):
        if token_list[token_id].normalized == "FROM":
            appending = True
            continue
        if appending and isinstance(token_list[token_id],Identifier):
            table = token_list[token_id].get_name()
            tables.append(table)
            from_tokens.append({"table":table})
        elif appending and token_list[token_id].ttype is Keyword:
            from_tokens.append({"keyword":token_list[token_id].normalized})
        if appending and isinstance(token_list[token_id],IdentifierList):
            from_tokens.append({"subquery":token_list[token_id].value})
        if isinstance(token_list[token_id],Where):
            appending=False
    return tables,from_tokens

def _extract_where_statement(token_list:list)->list:
    l=[]
    for token in token_list:
        if isinstance(token,Where):
            where_tokens = token.tokens
            queue = []
            for wt in where_tokens:
                if isinstance(wt,Parenthesis):
                    queue.append({"condition":wt.value})
                elif isinstance(wt,Comparison):
                    query = re.sub(r"\b\d+(\.\d+)?\b", "?", wt.normalized)  # Replace numbers
                    query = re.sub(r"\s+", " ", query).strip()  # Normalize whitespace
                    l.append({"condition":query})
                elif wt.ttype is Keyword and wt.normalized in ["AND","OR"]:
                    nl=[]
                    while queue:
                        nl.append(queue.pop())
                    l.append({"condition":nl})
                    l.append({"operator":wt.value})
                elif isinstance(wt,Identifier):
                    queue.append({"column":wt.value})
                elif wt.ttype is Keyword and wt.normalized != "WHERE":
                    queue.append({"keyword":wt.value})              
                elif wt.value in ["WHERE",""," ","\n","]"]:
                    continue
                else:
                    continue
                    print(wt)
            return l

def _extract_columns(token_list:list)->tuple[list,dict]:
    columns=[]
    appending = False
    columns_tokens = []
    for token_id in range(len(token_list)):
        if token_list[token_id].ttype is DML:
            appending = True
            continue
        if appending and isinstance(token_list[token_id],Identifier):
            column = token_list[token_id].value
            columns.append(column)
        elif appending and isinstance(token_list[token_id],Function):
            cond = token_list[token_id]
            columns.append(cond.value)
            columns_tokens.append({"function":cond.tokens[0].value,
                        "column":cond.tokens[1].value.replace("(","").replace(")","")})
        elif appending and isinstance(token_list[token_id],IdentifierList):
            for inner in token_list[token_id].tokens:
                if isinstance(inner,Identifier):
                    columns.append(inner.value)
        if token_list[token_id].normalized == "FROM":
            appending=False
    return columns, columns_tokens

def extract_structure(query:str):
    query_fixed = query.replace("\n"," ")
    struct = sqlparse.parse(query_fixed)[0]
    tokens = struct.tokens
    tables, from_tokens = _extract_from_statement(tokens)
    where_tokens = _extract_where_statement(tokens)
    columns, columns_tokens = _extract_columns(tokens)
    t=""
    for token in tokens:
        if token.normalized in ["SELECT","INSERT","INSERT INTO","UPDATE","DELETE"]:
            t = token.normalized
            break
    return {
        "type": t,
        "tables": json.dumps(",".join(tables)),
        "from_tokens": json.dumps(from_tokens),
        "where": json.dumps(where_tokens),
        "columns": json.dumps(",".join(columns)),
        "columns_tokens": json.dumps(columns_tokens),
    }

extract_udf = udf(extract_structure, StructType([
    StructField("type", StringType(), True),
    StructField("tables", StringType(), True),
    StructField("from_tokens", StringType(), True),
    StructField("where", StringType(), True),
    StructField("columns", StringType(), True),
    StructField("columns_tokens", StringType(), True)
]))


if __name__ == "__main__":
    import sys
    import re
    import tqdm
    from pyspark.sql import SparkSession

    test_select_query="""SELECT MIN(DTE_NUMBER) FROM DIAKDTES WHERE (ISSUE_FLAG=? AND DESTROY_FLAG=? 
        AND CANCEL_FLAG=?) AND (DIAKDTES.OWNER_ID = ? OR DIAKDTES.OWNER_ID = ?) AND 
        (DIAKDTES.ACCESS_CODE = ? OR DIAKDTES.ACCESS_CODE LIKE ?)"""

    query_with_join = """SELECT ELEGRESULT.ID,ELEG_LISTCODE,SHOW_ELEG_LISTCODE,TYPE_
    ELEG_LISTCODE,METRISI,APOT_TECHELEG_LISTCODE,IS_OLDVALUE,ELEG_REMARKS,YPOELEG1_LISTCODE,
    METRISI_YPOELEG1,METRISI_YPOELEG2,METRISI_YPOELEG3,METRISI_YPOELEG4,METRISI_YPOELEG5,
    METRISI_YPOELEG6,METRISI_YPOELEG7,METRISI_YPOELEG8,METRISI_YPOELEG9,METRISI_YPOELEG10,
    METRISI_YPOELEG11,METRISI_YPOELEG12,METRISI_YPOELEG13,METRISI_YPOELEG14,METRISI_YPOELEG15,
    METRISI_YPOELEG16,METRISI_YPOELEG17,METRISI_YPOELEG18,METRISI_YPOELEG19,METRISI_YPOELEG20,
    VEHSELEGS.LOOKUP_VALUE AS VEHSELEG,MOTOELEGS.LOOKUP_VALUE AS MOTOELEG 
    FROM ELEGRESULT LEFT OUTER JOIN JP_LOOKUPDATA VEHSELEGS ON 
    (SHOW_ELEG_LISTCODE=VEHSELEGS.LOOKUP_CODE AND VEHSELEGS.LOOKUP_NAME=?) 
    LEFT OUTER JOIN JP_LOOKUPDATA MOTOELEGS ON (SHOW_ELEG_LISTCODE=MOTOELEGS.LOOKUP_
    CODE AND MOTOELEGS.LOOKUP_NAME=?) WHERE (ELEGRESULT.ID = ?) AND (ELEGRESULT.EISODOS_ID = ?)
    AND (ELEGRESULT.OWNER_ID = ? OR ELEGRESULT.OWNER_ID = ?) AND (ELEGRESULT.ACCESS_CODE = ? OR
    ELEGRESULT.ACCESS_CODE LIKE ?) ORDER BY APOT_TECHELEG_LISTCODE DESC, TYPE_ELEG_LISTCODE"""

    struct= sqlparse.parse(test_select_query)[0]
    tokens = struct.tokens

    f = extract_structure(test_select_query)
    #test on the entire dataset to see if the code crushes
    log_file = sys.argv[1] if len(sys.argv) > 1 else "input/audit/20241210.log"

    data=[]
    with open(log_file, 'r', encoding='windows-1253', errors='replace') as f:
        for line in f:
            # print(line)           # uncomment to see if encoding is correct
            if  "Ελεγκτης" in line and line[0]=="[" and ("QUERY" in line or "INSERT INTO" in line or "UPDATE" in line):
                if "QUERY" in line:
                    query = re.sub(r"'.*?'", "?", line.split("QUERY")[1].replace(":",""))
                elif "INSERT INTO" in line:
                    query = "INSERT INTO " + re.sub(r"'.*?'", "?", line.split("INSERT INTO")[1].replace(":",""))
                else:
                    query = "UPDATE " + re.sub(r"'.*?'", "?", line.split("UPDATE")[1].replace(":","")) 
                data.append(query)


    schema = StructType([StructField("line", StringType(), True)])

    spark = SparkSession.builder \
        .appName("KTEO log experiments") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    log_lines=spark.createDataFrame([(s,) for s in data], schema=schema)

    df = (log_lines
      .withColumn("extracted", extract_udf(col("line")))
      .select(
          col("extracted.type").alias("type"),
          col("extracted.tables").alias("tables"),
          col("extracted.from_tokens").alias("from_tokens"),
          col("extracted.where").alias("where"),
          col("extracted.columns").alias("columns"),
          col("extracted.columns_tokens").alias("columns_tokens")
      ))\
    .filter(col("type")=="SELECT")\
    .distinct()\
    .coalesce(1)
    
    output_file = "output/extract_structure_sql"
    df.write.csv(output_file, header=True, mode="overwrite")
    
    