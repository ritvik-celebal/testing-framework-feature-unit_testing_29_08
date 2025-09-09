from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, when, lit, current_timestamp, to_timestamp, regexp_replace, trim, lower, upper, concat_ws, hash, coalesce, struct, to_json, from_json, array, explode, size, collect_list, sum as spark_sum, count as spark_count, max as spark_max, min as spark_min, avg, desc, asc, expr, to_date, substring, date_format, decode, from_utc_timestamp, count, collect_set, pandas_udf
import json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Callable, Tuple
import time 
import random
import pandas as pd
import re
import logging
# from ..utils import schemas
# from ..utils.schemas import JsonSchemaValidator
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import Mock
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("eventhub_session").getOrCreate()
try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    dbutils = None

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

validate_schema = StructType([
    StructField("is_valid", BooleanType(), True),
    StructField("error", StringType(), True),
])

# ---------- Pandas UDF for Schema Validation ----------
def create_validation_udf(validators_dict: Dict[str, any]) -> Callable[[pd.Series, pd.Series], pd.DataFrame]:
    try:
        @pandas_udf(validate_schema)
        def validate_all(json_series: pd.Series, hub_series: pd.Series) -> pd.DataFrame:
            is_valid_list, error_list = [], []
            for json_str, hub in zip(json_series, hub_series):
                try:
                    obj = json.loads(json_str)
                    validators_dict[hub].validate_instance(obj)
                    is_valid_list.append(True)
                    error_list.append(None)
                except Exception as e:
                    is_valid_list.append(False)
                    error_list.append(str(e))
            return pd.DataFrame({"is_valid": is_valid_list, "error": error_list})

        return validate_all
    except Exception as e:
        logger.error(f"Failed to create validation UDF: {e}")
        raise

def get_eventhub_list(catalog_name: str, eventhub_namespace: str, schema_name: str) -> List[str]:
    """
    Fetch list of raw table names (Event Hubs) from lookup table for a given namespace.
    """
    query = f"""
        SELECT distinct table_name
        FROM {catalog_name}.{schema_name}.lookup_table_source2raw
        WHERE source_name = 'EVENTHUB'
          AND eventhub_namespace = '{eventhub_namespace}'
    """
    try:
        df = spark.sql(query)
        event_hubs_list = [
            row["table_name"] for row in df.select("table_name").collect()
        ]
        logger.info(
            f"Fetched Event Hub tables for eventhub namespace '{eventhub_namespace}': {event_hubs_list}"
        )
        return event_hubs_list
    except Exception as e:
        logger.error(f"Failed to fetch Event Hub list for namespace {eventhub_namespace}: {e}")
        raise

def get_validators(event_hubs_list: List[str]) -> Dict[str, any]:
    """
    Dynamically fetch validator objects from the schemas module for each hub.
    """
    try:
        validators = {
            hub: getattr(schemas, hub)
            for hub in event_hubs_list
            if hasattr(schemas, hub)  # Only include if the schema is defined
        }
        logger.info(f"Validators loaded for hubs: {list(validators.keys())}")
        return validators
    except Exception as e:
        logger.error(f"Failed to build validators from schemas module: {str(e)}")
        raise

def read_eventhub_stream(hubs: str,bootstrap_servers: str,eh_sasl: str) -> DataFrame:
    """
    Creates a Kafka-based Spark readStream using the given comma-separated list of topics (Event Hubs).
    """
    try:
        return (
            spark.readStream.format("kafka")
            .option("subscribe", hubs)
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("startingOffsets", "earliest")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", eh_sasl)
            .option("failOnDataLoss", "false")
            .option("kafka.request.timeout.ms", "60000")
            .option("kafka.session.timeout.ms", "30000")
            .load()
        )
    except Exception as e:
        logger.error(f"Failed to start Event Hub stream for hubs '{hubs}': {e}")
        raise

def decode_stream_data(df: DataFrame) -> DataFrame:
    """
    Decodes Kafka messages to readable JSON string and extracts topic name as `hub`.
    """
    try:
        return df.select(
            col("topic").alias("hub"),
            decode(col("value"), "UTF-8").cast("string").alias("json_str"),
        )
    except Exception as e:
        logger.error(f"Failed to decode Kafka stream data: {e}")
        raise

def write_valid_invalid_streams(
    hub: str,
    decoded_df: DataFrame,eventhub_raw_checkpoint: str,
    eventhub_invalid_records_checkpoint: str,
    eventhub_raw_path: str,
    eventhub_invalid_records_path: str,
    validate_all: Callable[[pd.Series, pd.Series], pd.DataFrame],
    stream_queries: List[Tuple[str, StreamingQuery]]
) -> None:
    """
    For a given hub, splits valid and invalid records and writes them to raw and invalid paths respectively.
    Appends the active stream queries to `stream_queries` for monitoring.
    """
    # Apply schema validation using UDF
    validated_df = (
        decoded_df.withColumn("validation", validate_all(col("json_str"), col("hub")))
        .withColumn("is_valid", col("validation.is_valid"))
        .withColumn("error", col("validation.error"))
        .drop("validation")
    )

    # Filter only records from this hub
    filtered_df = validated_df.filter(col("hub") == hub)

    # Separate valid records
    valid_df = filtered_df.filter(col("is_valid") == True).drop(
        "is_valid", "error", "hub"
    )

    # Add timestamp to invalid records
    invalid_df = (
        filtered_df.filter(col("is_valid") == False)
        .withColumn(
            "ingestion_time",
            from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh"),
        )
        .drop("is_valid", "hub")
    )

    # Write valid records to raw path (as text)
    try:
        write_valid = (
            valid_df.writeStream.format("text")
            .option("checkpointLocation", f"{eventhub_raw_checkpoint}/{hub}")
            .option("path", f"{eventhub_raw_path}/{hub}")
            .outputMode("append")
            .trigger(processingTime="15 minutes")
            .start()
        )
    except Exception as e:
        logger.error(f"Failed to start write stream for valid records of {hub}: {e}")
        raise

    # Write invalid records to Delta
    try:
        write_invalid = (
        invalid_df.writeStream.format("delta")
        .option("checkpointLocation", f"{eventhub_invalid_records_checkpoint}/{hub}")
        .option("path", f"{eventhub_invalid_records_path}/{hub}")
        .outputMode("append")
        .trigger(processingTime="15 minutes")
        .start()
        )
    except Exception as e:
        logger.error(f"Failed to start write stream for invalid records of {hub}: {e}")
        raise

    # Store queries for later monitoring
    stream_queries.extend(
        [(f"{hub}_raw_stream", write_valid), (f"{hub}_invalid_stream", write_invalid)]
    )

def start_bronze_stream_for_hub(
    hub: str,
    catalog_name: str,
    bronze_schema: str,eventhub_bronze_schema_location: str,
    eventhub_raw_path: str,eventhub_bronze_checkpoint: str,schema_name: str,
    stream_queries: List[Tuple[str, StreamingQuery]]
) -> None:
    """
    Reads raw data from Event Hub text files and writes to a Delta table in Bronze layer.
    Handles dynamic or missing `ProcDate` field.
    """
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{eventhub_bronze_schema_location}/{hub}")
        .option("mergeSchema", "true")
        .load(f"{eventhub_raw_path}/{hub}")
    )

    # Normalize or add ProcDate
    if "ProcDate" not in df.columns:
        df = df.withColumn(
            "ProcDate",
            date_format(
                from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh"),
                "yyyyMMddHHmmss",
            ),
        )
    else:
        df = df.withColumn(
            "ProcDate",
            when(col("ProcDate").rlike("^[0-9]{14}$"), col("ProcDate")).otherwise(
                date_format(
                    to_timestamp(col("ProcDate"), "yyyy-MM-dd HH:mm:ss"),
                    "yyyyMMddHHmmss",
                )
            ),
        )
    df=df.withColumn(
                "source_metadata",
                concat_ws(
                    "|",
                    col("_metadata.file_path"),
                    from_utc_timestamp(col("_metadata.file_modification_time"), "Asia/Ho_Chi_Minh").cast("string")
                )
            )

    def write_batch_with_rowcount_audit(batch_df, batch_id):
        try:
            if batch_df.count()>0:
                batch_df.write.format("delta").mode("append").partitionBy("ProcDate").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{bronze_schema}.{hub}")
                batch_ts = from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh")
                audit_df = (
                    batch_df
                    .withColumn("proc_date", substring("ProcDate", 1, 8))  # extract yyMMdd from yyyyMMddHHmmss
                    .groupBy("proc_date")
                    .agg(
                        count("*").alias("row_count")
                    )
                    .withColumn("hub", lit(hub))
                    .withColumn("batch_id", lit(batch_id))
                    .withColumn("batch_timestamp", lit(batch_ts))
                )

                audit_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.eventhub_audit_log")
        except Exception as e:
            logger.error(f"Failed to write audit log for {hub}: {e}")
            raise

    # Write to Bronze Delta table
    try:
        write_bronze = (
            df.writeStream
            .foreachBatch(write_batch_with_rowcount_audit)
            .option("checkpointLocation", f"{eventhub_bronze_checkpoint}/{hub}")
            .trigger(processingTime="15 minutes")
            .start()
        )
        logger.info(f"Bronze stream started for {hub}")
        write_bronze_marker_if_missing(hub,eventhub_raw_path)

    except Exception as e:
        logger.error(f"Failed to start write bronze stream of {hub}: {e}")
        raise
    stream_queries.append((f"{hub}_bronze_stream", write_bronze))

def write_bronze_marker_if_missing(hub: str,eventhub_raw_path: str) -> None:
    """
    Writes a marker file named `_bronze_started` in the Event Hub's raw directory if it doesn't already exist.
    This marker indicates that the Bronze stream has been initiated for the given hub.
    """
    marker_path = f"{eventhub_raw_path}/{hub}/_bronze_started"
    try:
        if not any(f.name == "_bronze_started" for f in dbutils.fs.ls(f"{eventhub_raw_path}/{hub}")):
            dbutils.fs.put(marker_path, "", overwrite=False)
            logger.info(f"Wrote bronze started marker for hub: {hub}")
    except Exception as e:
        logger.error(f"Failed to write bronze marker for {hub}: {e}")
        raise

def is_data_available(path: str) -> bool:
    """
    Checks whether raw data exists or bronze already started for this hub.
    """
    try:
        if any(f.name == "_bronze_started" for f in dbutils.fs.ls(path)):
            return True  # Skip expensive listing if already started
        files = dbutils.fs.ls(path)
        return any(
            f.size > 0 and not f.name.startswith("_") and not f.name.startswith(".")
            for f in files
        )
    except Exception as e:
        logger.error(f"Failed to check files at path {path}: {e}")
        return False

def check_and_start_stream(hub: str,catalog_name: str, bronze_schema: str,eventhub_bronze_schema_location: str,
    eventhub_raw_path: str,eventhub_bronze_checkpoint: str,schema_name: str, stream_queries: List[Tuple[str, StreamingQuery]]) -> None:
    """
    Keep checking until data is available for the hub.
    Once data is found, trigger the Bronze stream.
    """
    path = f"{eventhub_raw_path}/{hub}"
    while True:
        if is_data_available(path=path):
            logger.info(f"Data found for hub: {hub} — starting bronze stream")
            try:
                start_bronze_stream_for_hub(hub, catalog_name, bronze_schema,eventhub_bronze_schema_location,
    eventhub_raw_path,eventhub_bronze_checkpoint,schema_name, stream_queries)
            except Exception as e:
                logger.error(f"Error starting bronze stream for {hub}: {e}")
            break  # Exit loop after starting stream
        else:
            logger.info(f"Waiting for data for hub: {hub}. Retrying in 60s...")
            time.sleep(60)

def monitor_streams(
    queries: List[Tuple[str, StreamingQuery]]
) -> None:
    """
    Monitors multiple Spark Streaming queries in parallel.

    """
    def wait_for_query(name, query):
        try:
            query.awaitTermination()
        except Exception as e:
            logger.error(f"Streaming query '{name}' failed: {e}")
            raise RuntimeError(f"Query '{name}' terminated with error.")

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(wait_for_query, name, query) for name, query in queries]
        for f in futures:
            f.result()

def get_all_eventhub_list(catalog_name: str,schema_name: str) -> List[str]:
    """
    Fetch list of raw table names (Event Hubs) from lookup table for a given namespace.
    """
    query = f"""
        SELECT distinct table_name
        FROM {catalog_name}.{schema_name}.lookup_table_source2raw
        WHERE source_name = 'EVENTHUB'
    """
    try:
        df = spark.sql(query)
        event_hubs_list = [
            row["table_name"] for row in df.select("table_name").collect()
        ]
        logger.info(f"Fetched Event Hub tables: {event_hubs_list}")
        return event_hubs_list
    except Exception as e:
        logger.error(f"Failed to fetch Event Hub list: {e}")
        raise

def get_invalid_record_summary(
    hubs: List[str],
    invalid_path: str,
    proc_date: str
) -> DataFrame:
    """
    Returns schema validation summary for each Event Hub.
    Calculates number of invalid records for a given date,
    and identifies if schema mismatches occurred with corresponding error messages.
    """
    audit_rows = []
    for hub in hubs:
        logger.info(f"Checking hub: {hub}")
        try:
            df = spark.read.format("delta").load(f"{invalid_path}/{hub}/")
            df = df.withColumn("ingestion_time", to_date(col("ingestion_time"), "yyyyMMdd"))
            filtered_df = df.filter(col("ingestion_time") == lit(proc_date))
            invalid_count = filtered_df.count()
            status = "Mismatched" if invalid_count > 0 else "Matched"
            error_str = (
                filtered_df.agg(concat_ws(", ", collect_set("error")).alias("error_string")).collect()[0]["error_string"]
                if invalid_count > 0 else None
            )
            audit_rows.append((hub, datetime.now(timezone(timedelta(hours=7))), invalid_count, status, error_str))
        except Exception as e:
            logger.error(f"Error reading data for hub '{hub}': {str(e)}")
            audit_rows.append((hub, datetime.now(timezone(timedelta(hours=7))), -1, "ERROR", str(e)))

    audit_schema = StructType([
        StructField("hub", StringType(), True),
        StructField("audit_time_utc", TimestampType(), True),
        StructField("invalid_record_count", IntegerType(), True),
        StructField("schema_match_status", StringType(), True),
        StructField("schema_mismatches", StringType(), True)
    ])
    return spark.createDataFrame(audit_rows, audit_schema)

def get_bronze_summary(proc_date: str, catalog_name: str, schema_name: str, hubs: List[str]) -> DataFrame:
    """
    Returns Bronze layer summary for a given proc_date.
    Aggregates row count from the audit log per hub for the specified processing date.
    """
    bronze_audit_df = spark.table(f"{catalog_name}.{schema_name}.eventhub_audit_log")
    actual_summary = (
        bronze_audit_df.filter(col("proc_date") == proc_date)
        .select("proc_date", "row_count", "hub")
        .groupBy("hub", "proc_date")
        .agg(spark_sum("row_count").alias("streaming_df_count"))
    )

    # Add 0-count rows for hubs that didn't appear
    missing_hubs = set(hubs) - set([row["hub"] for row in actual_summary.select("hub").distinct().collect()])
    missing_df = spark.createDataFrame([(hub, proc_date, 0) for hub in missing_hubs], actual_summary.schema)
    return actual_summary.unionByName(missing_df)

def get_delta_summary(hubs: List[str], proc_date: str, catalog_name: str, bronze_schema: str) -> DataFrame:
    """
    Returns Delta table summary for each Event Hub.
    Aggregates row count per hub and date from Delta tables matching the proc_date.
    """
    all_summaries = []
    for hub in hubs:
        try:
            logger.info(f"Processing delta table for hub: {hub}")
            delta_df = spark.table(f"{catalog_name}.{bronze_schema}.{hub}")
            delta_df = delta_df.withColumn("proc_date", col("ProcDate").substr(1, 8))
            filtered_df = delta_df.filter(col("proc_date") == proc_date)
            summary = (
                filtered_df.groupBy("proc_date")
                .agg(count("*").alias("delta_row_count"))
                .withColumn("hub", lit(hub))
            )
            all_summaries.append(summary)
        except Exception as e:
            logger.error(f"Error processing hub {hub}: {str(e)}")
    if not all_summaries:
        raise ValueError("No delta summaries were generated.")
    df = all_summaries[0]
    for other_df in all_summaries[1:]:
        df = df.unionByName(other_df)
    return df

def build_final_audit_df(
    bronze_df: DataFrame,
    delta_df: DataFrame,
    audit_df: DataFrame
) -> DataFrame:
    """
    Combines Bronze, Delta, and Invalid record summaries into a single audit DataFrame
    with metrics: streaming row count, delta row count, invalid record count, and schema status.
    """
    return (
        bronze_df.alias("bronze")
        .join(delta_df.alias("delta"), ["hub", "proc_date"], "outer")
        .join(audit_df.alias("audit"), "hub", "left")
        .select(
            "bronze.hub", "bronze.proc_date",
            "bronze.streaming_df_count", "delta.delta_row_count",
            col("audit.invalid_record_count"),
            col("audit.schema_match_status"),
            col("audit.schema_mismatches")
        )
    )

def write_to_audit_table(
    final_df: DataFrame,
    catalog_name: str,schema_name: str,
    proc_date_col: str = "proc_date"
):
    """
    Writes the final audit DataFrame to the consolidated audit result Delta table
    after enriching with metadata fields like source info, load date, and pipeline name.
    """
    target_schema = spark.table(f"{catalog_name}.{schema_name}.consolidate_audit_result").schema

    enriched_df = final_df \
        .withColumn("load_date", to_date(col(proc_date_col), "yyyyMMdd")) \
        .withColumn("source_name", lit("EventHub")) \
        .withColumn("source_description", lit("EventHub")) \
        .withColumn("table_name", col("hub")) \
        .withColumn("pipeline_name", lit("eventhub_source_to_bronze_workflow")) \
        .withColumn("environment", lit("dev")) \
        .withColumn("source_object_count", col("streaming_df_count").cast("string")) \
        .withColumn("target_object_count", col("delta_row_count").cast("string"))

    for field in target_schema:
        if field.name not in enriched_df.columns:
            enriched_df = enriched_df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            enriched_df = enriched_df.withColumn(field.name, col(field.name).cast(field.dataType))

    final_insert_df = enriched_df.select(*[field.name for field in target_schema])
    final_insert_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.consolidate_audit_result")
    return final_insert_df
