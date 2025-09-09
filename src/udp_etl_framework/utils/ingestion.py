# Databricks notebook source
##  import required libraries and functions
import sys
import requests
import re
import ast
import pytz
import json
import os
import time
import random
import uuid
import json
import numpy as np
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce
from delta.tables import DeltaTable
from datetime import datetime, timedelta, timezone
import calendar
import requests
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import builtins as P
import logging
import decimal

# COMMAND ----------

dbutils.widgets.text("environment", "", "")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# Configure logging
vietnam_tz = timezone(timedelta(hours=7))
logging.Formatter.converter = lambda *args: datetime.now(vietnam_tz).timetuple()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

def read_json(path: str) -> DataFrame:
    """Reads a multiline JSON file into a Spark DataFrame."""
    return spark.read.option("multiline", "true").json(path)

def list_folder(path: str) -> List:
    """Lists the contents of a folder in DBFS or mounted storage."""
    return dbutils.fs.ls(path)

def read_csv(path: str, header: str, delimiter: str) -> DataFrame:
    """Reads a CSV file into a Spark DataFrame with specified header and delimiter."""
    return spark.read.option("header", header) \
                .option("delimiter", delimiter) \
                .csv(path)

def write_to_temp(df: DataFrame, temp_path: str) -> None:
    """Writes a DataFrame to a temporary location in Snappy Parquet format."""
    df.coalesce(1).write.mode("append").option("compression", "snappy").parquet(temp_path)

def sum_columns(df: DataFrame, columns: Union[str, List[str]]) -> Union[str, None]:
    """Calculates the sum of one or more numeric columns and returns it as a JSON string."""
    if columns == '':
        return None
    else:
        if isinstance(columns, str):
            columns = [col.strip() for col in columns.split(",")]
        sums = df.select([sum(col).alias(col) for col in columns]).collect()[0]
        return json.dumps({col: sums[col] for col in columns}, ensure_ascii=False)

def to_datetime_object(timestamp: str) -> datetime:
    """Converts a timestamp string into a Python datetime object."""
    if 'T' in timestamp:
        timestamp = timestamp[:26]
        return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    else:
        return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')

# COMMAND ----------

def compare_dicts(dict1: dict, dict2: dict) -> str:
    """Compares two dictionaries for exact match."""
    if not dict1 or not dict2:
        return "Failed"
    if dict1 == dict2:
        return "Passed"
    return "Failed"

def compare_counts(count1: int, count2: int) -> str:
    """Compares two count values for equality."""
    if not count1 or not count2:
        return 'Failed'
    if count1 == count2:
        return "Passed"
    return "Failed"

def get_qa_status(
    validations: List[Tuple[Callable[[Any, Any], str], Any, Any, str]]
) -> Tuple[str, List[Tuple[str, str]]]:
    """Runs multiple validations and returns overall status with messages."""
    status_list = []
    is_validation_failed = False
    for func, arg1, arg2, label in validations:
        result = func(arg1, arg2)
        if result == "Failed":
            is_validation_failed = True
            status_list.append(('Failed', f"Validation Failed - {label}"))
        else:
            status_list.append((result, f"Validation Passed - {label}"))

    if is_validation_failed:
        return "Failed", status_list
    else:
        return "Passed", status_list


# COMMAND ----------

def copy_with_rename(source_dir: str, target_dir: str, pipeline_trigger_time: str, partition_column: str = 'current_date') -> None:
    """Copies Parquet files from source to target with a renamed timestamped filename and partitioned path."""
    
    if partition_column == 'current_date':
        cnt = 0
        pipeline_trigger_time = to_datetime_object(pipeline_trigger_time)
        pipeline_trigger_time = pipeline_trigger_time + timedelta(hours=7)
        timestamp = (datetime.now() + timedelta(hours=7)).strftime("%Y%m%d_%H%M%S")

        files = dbutils.fs.ls(source_dir)

        target_dir = f"{target_dir}/yyyy={pipeline_trigger_time.year}/mm={pipeline_trigger_time.month:02}/dd={pipeline_trigger_time.day:02}"
        for file_info in files:
            path = file_info.path
            if path.endswith(".parquet"):
                filename = path.split("/")[-1]
                filename_part = filename.replace(".c000.snappy.parquet", "").replace("c000.snappy.parquet", "")
                new_filename = f"{filename_part}_{timestamp}.snappy.parquet"
                new_path = f"{target_dir}/{new_filename}"

                # Copy and rename
                dbutils.fs.cp(path, new_path)
                cnt += 1

        print(f"Timestamp in VNT:{timestamp}\n")
        print(f"""For reference: File renamed from 
            {filename}
            to 
            {new_filename}.\n""")
        print(f"""{cnt} files copied from 
            {source_dir} 
            to 
            {target_dir}.\n""")
    else:
        pass


# COMMAND ----------

def read_from_adls(stg_path: str) -> tuple[DataFrame, list[str]]:
    """Reads a Parquet dataset from ADLS and returns the DataFrame along with file paths."""
    try:
        df = spark.read.parquet(stg_path)
        file_paths = [row.path for row in dbutils.fs.ls(stg_path)]
    except Exception as e:
        print('No new file found')
        schema = StructType([
            StructField("Error", StringType(), True)
        ])
        df = spark.createDataFrame([str(e)], schema)
        file_paths = []
    return df, file_paths

def modified_partition(df: DataFrame, partition_column: str) -> DataFrame:
    """Adds partition columns (yyyy, mm, dd) derived from the specified column and adds a creation timestamp."""
    df = df.withColumn("file_creation_ts", current_timestamp() + expr("INTERVAL 7 HOURS"))
    
    normalized_date = to_date(
        substring(regexp_replace(df[partition_column].cast("string"), "-", ""), 1, 8), "yyyyMMdd"
    )
    df = df \
        .withColumn("__partition_date", normalized_date) \
        .withColumn("yyyy", date_format("__partition_date", "yyyy")) \
        .withColumn("mm", date_format("__partition_date", "MM")) \
        .withColumn("dd", date_format("__partition_date", "dd")) \
        .drop("__partition_date")

    return df

def write_to_adls(df: DataFrame, table_name: str, partition_column: str, destination_path: str) -> None:
    """Writes the DataFrame to ADLS as partitioned Parquet files using yyyy/mm/dd columns."""
    repartition_count = int(df.select(partition_column).distinct().count())
    if repartition_count == 0:
        repartition_count = 1
    df.repartition(repartition_count, "yyyy", "mm", "dd") \
        .write \
        .partitionBy("yyyy", "mm", "dd") \
        .format("parquet") \
        .mode("append") \
        .save(destination_path)

def delete_temp_folder(path: str) -> None:
    """Deletes the temporary folder from DBFS or ADLS if it exists."""
    try:
        dbutils.fs.ls(path)
        dbutils.fs.rm(path, recurse=True)
        print(f"Deleted temp folder: {path}")
    except:
        print(f"Temp folder does not exist: {path}")


# COMMAND ----------

config = open("../configs/config.json")
settings = json.load(config)

# COMMAND ----------

catalogName = settings[environment]['catalogName']

# COMMAND ----------

import time
import random
from pyspark.sql import SparkSession

def concurrency_confict_handler(query: str) -> str:
    """Executes a Spark SQL query with retry logic for concurrency conflicts."""
    max_retries = 10
    retries = 0

    while retries < max_retries:
        try:
            spark.sql(query)
            return "Insert/Update Successful"
        except Exception as e:
            error_message = str(e)

            # Check for concurrency-related issues
            if "concurrent" in error_message.lower() or "concurrency" in error_message.lower():
                wait_time = random.randint(1, 5)
                print(f"Concurrency conflict detected. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
            else:
                raise e  # Re-raise non-concurrency exceptions

    raise Exception("Max retries reached. Concurrency issue persists.")


# COMMAND ----------

def directory_exists(path: str) -> bool:
    """Checks if a given directory path exists in DBFS or ADLS."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False
