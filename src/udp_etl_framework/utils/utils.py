from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, current_timestamp, to_timestamp, regexp_replace, trim, lower, upper, concat_ws, hash, coalesce, struct, to_json, from_json, array, explode, size, collect_list, sum as spark_sum, count as spark_count, max as spark_max, min as spark_min, avg, desc, asc, expr, to_date, substring, date_format, decode, from_utc_timestamp, count, collect_set
import json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime, timedelta, timezone
import time
import random
import farmhash
import pandas as pd
import numpy as np
import re
import logging
from unittest.mock import Mock

# Mock JsonSchemaValidator class
class JsonSchemaValidator:
    def __init__(self, schema):
        self.schema = schema
        
    def validate_instance(self, instance):
        # Mock validation - just return True for testing
        return True

# Create some mock schema validators that might be used in utils.py
data_portal = Mock()
another_missing = Mock()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def sum_columns(df, columns):
    # df -> dataframe
    # columns -> string or list of columns
    if columns == '':
        return None
    else:
        if isinstance(columns, str):
            columns = [col.strip() for col in columns.split(",")]
        sums = df.select([spark_sum(col).alias(col) for col in columns]).collect()[0]
        return json.dumps({col: sums[col] for col in columns}, ensure_ascii=False)

def compare_dicts(dict1, dict2):
    if not dict1 or not dict2:
        return "Failed"
    if dict1 == dict2:
        return "Passed"
    return "Failed"

def compare_counts(count1, count2):
    if not count1 or not count2:
        return 'Failed'
    if count1 == count2:
        return "Passed"
    return "Failed"

def get_qa_status(validations):
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
    
def read_from_adls(dbutils, stg_path: str):
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

def modified_partition(df: DataFrame , partition_column: str):
    df = df.withColumn("file_creation_ts",current_timestamp() + expr("INTERVAL 7 HOURS"))
    
    normalized_date = to_date(substring(regexp_replace(df[partition_column].cast("string"), "-", ""), 1, 8),"yyyyMMdd")
    df = df \
    .withColumn("__partition_date", normalized_date) \
    .withColumn("yyyy", date_format("__partition_date", "yyyy")) \
    .withColumn("mm", date_format("__partition_date", "MM")) \
    .withColumn("dd", date_format("__partition_date", "dd")) \
    .drop("__partition_date")

    return df

def to_datetime_object(timestamp: str):
    if 'T' in timestamp:
        timestamp = timestamp[:26]
        return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    else:
        return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    
def concurrency_confict_handler(query):
    max_retries = 10
    retries = 0

    while retries < max_retries:
        try:
            spark.sql(query)
            return "Insert/Update Successful"
        except Exception as e:
            error_message = str(e)
            
            # Check if the error message contains concurrency-related keywords
            if "concurrent" in error_message.lower() or "concurrency" in error_message.lower():
                wait_time = random.randint(1, 5)
                print(f"Concurrency conflict detected. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
            else:
                raise e  # Raise the error if it's not a concurrency issue
    
    raise Exception("Max retries reached. Concurrency issue persists.")

def copy_with_rename(source_dir, target_dir, pipeline_trigger_time, partition_column = 'current_date'):
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
                filename_part = filename.replace(".c000.snappy.parquet", "").replace("c000.snappy.parquet","")
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

def fingerprint_signed(value):
    if value is None:
        return None
    return int(np.uint64(farmhash.fingerprint64(value)).astype("int64"))

def extract(filepath, source_file_format, schema=None, rootTag="root") -> "DataFrame":
    reader = (
        spark.read.format(f"{source_file_format}")
        .option("rowTag", f"{rootTag}")
        .option("includeFileName", "true")
    )
    if schema:
        reader = reader.schema(schema)
    df = reader.load(filepath)
    df_src_col = df.columns
    df_src_col_cnt = len(df_src_col)
    return df, df_src_col_cnt

def clean_column_names(df):
    for col_name in df.columns:
        clean_name = re.sub(r"[^A-Za-z0-9]+", "_", col_name)
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
    return df

def read_csv_src(last_refresh_date_mod, pipeline_trigger_time_mod, agg_columns_str, delimiter_str, path_src):
    src_df = (
        spark.read.format("csv")
        .option("sep", f"{delimiter_str}")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("modifiedAfter", f"{last_refresh_date_mod}")
        .option("modifiedBefore", f"{pipeline_trigger_time_mod}")
        .load(path_src)
    )
    src_count = src_df.count()
    df_src_col = src_df.columns
    df_src_col_cnt = len(df_src_col)
    return src_count, src_df, df_src_col_cnt

def read_parquet_tgt(pipeline_trigger_time_mod, agg_columns_str, path_tgt):
    tgt_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("modifiedAfter", f"{pipeline_trigger_time_mod}")
        .load(path_tgt)
    )
    tgt_count = tgt_df.count()
    df_tgt_col = tgt_df.columns
    df_tgt_col_cnt = len(df_tgt_col)
    return tgt_count, tgt_df, df_tgt_col_cnt


def identify_new_files(source_description: str,path: str, last_refresh_date: str, pipeline_trigger_time: str):
    logger.info(f"Source Path:{path}")
    new_files = []
    new_files_size = []
    files_df = None
 
    pipeline_trigger_time = to_datetime_object(pipeline_trigger_time)
 
    logger.info(f"pipeline_trigger_time:{pipeline_trigger_time}")
    pipeline_trigger_time_epoc = pipeline_trigger_time.timestamp()
 
    last_refresh_date = to_datetime_object(last_refresh_date)
 
    last_refresh_date = last_refresh_date - timedelta(hours=7)
    logger.info(f"last_refresh_date:{last_refresh_date}")
    last_refresh_date_epoc = last_refresh_date.timestamp()
 
    filter_date = last_refresh_date - timedelta(days=1)
    filter_date_epoc = filter_date.timestamp()
 
    if source_description == "cxloyalty":
        try:
            sub_folders_df = spark.createDataFrame(list_folder(path))
        except:
            raise Exception("No sub-folders found at the source path:", path)
           
        filtered_folders_df = sub_folders_df.filter(col("modificationTime") / 1000 > filter_date_epoc)
        for row in filtered_folders_df.collect():
            if files_df == None:
                files_df = spark.createDataFrame(list_folder(row.path))
            else:
                temp_df = spark.createDataFrame(list_folder(row.path))
                files_df = files_df.union(temp_df)
    else:
        files_df = spark.createDataFrame(list_folder(path))
    if str(last_refresh_date.date()) == '1900-01-01':
        filter_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        filter_date_epoc = filter_date.timestamp()
        new_files_df = files_df.filter((col("modificationTime") / 1000 >= filter_date_epoc) & (col("modificationTime") / 1000 < pipeline_trigger_time_epoc))
    else:
        new_files_df = files_df.filter((col("modificationTime") / 1000 >= last_refresh_date_epoc) & (col("modificationTime") / 1000 < pipeline_trigger_time_epoc))
    new_files.extend([(row.path) for row in new_files_df.collect() if row.path.endswith(f".csv") or row.path.endswith(f".CSV")])
    return new_files
