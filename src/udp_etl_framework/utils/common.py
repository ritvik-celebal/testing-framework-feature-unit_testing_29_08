# Databricks notebook source
# MAGIC %run "../common/common_date_param"

# COMMAND ----------

dbutils.widgets.text("proc_date", "")
proc_date = dbutils.widgets.get("proc_date")
print(proc_date)

# COMMAND ----------

dbutils.widgets.text("environment", "", "")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %pip install pyfarmhash
# MAGIC %pip install numpy
# MAGIC %pip install croniter

# COMMAND ----------

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# COMMAND ----------

import sys
import os
import re
import ast
import time
import uuid
import json
import pytz
import random
import calendar
import logging
import requests

import numpy as np
import farmhash
from croniter import croniter

from datetime import datetime, timedelta, timezone

from pytz import timezone as pytz_timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException, StreamingQueryException
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    DateType, DecimalType, TimestampType, DoubleType, BooleanType, LongType
)

from pyspark.sql.functions import (
    col, lit, to_date, to_timestamp, current_timestamp,
    decode, udf
)

from delta.tables import DeltaTable


# COMMAND ----------

@udf()
def farm_fingerprint(value):
    return int(np.uint64(farmhash.fingerprint64(value)).astype('int64'))
spark.udf.register("farm_fingerprint", farm_fingerprint)

# COMMAND ----------

import os
current_directory = os.getcwd()
current_directory

if "silver_to_gold" in current_directory:
    print("Layer: 'GOLD'")
    config = open("../configs/config.json")
    settings = json.load(config)
elif "raw_to_silver/bronze" in current_directory:
    print("Layer: 'BRONZE'")
    config = open("../../configs/config.json")
    settings = json.load(config)
elif "raw_to_silver/silver" in current_directory:
    print("Layer: 'SILVER'")
    config = open("../../../configs/config.json")
    settings = json.load(config)
elif "common" in current_directory:
    print("Layer: 'COMMON'")
    config = open("../configs/config.json")
    settings = json.load(config)
else:
    raise ValueError(f"UNABLE TO LOCATE CONFIG FILE")

# COMMAND ----------

catalog_name = settings[environment]['catalog_name']

# COMMAND ----------

# MAGIC %md
# MAGIC These 2 functions will be used in validate dependancy notebook

# COMMAND ----------

from croniter import croniter
from datetime import datetime, timedelta

def classify_cron_job(cron_expression):
    try:
        base_time = datetime(2024, 6, 1, 0, 0, 0)  # Fixed reference time
        
        # Count runs in one day
        day_start = base_time
        day_end = day_start + timedelta(days=1)
        daily_runs = count_runs_with_croniter(cron_expression, day_start, day_end)
        
        if daily_runs == 1:
            return 'Daily'
        elif daily_runs > 1:
            return 'Hourly'
        else:
            # Check if it runs once per month
            month_start = base_time
            month_end = month_start + timedelta(days=31)
            monthly_runs = count_runs_with_croniter(cron_expression, month_start, month_end)
            
            if monthly_runs == 1:
                return 'Monthly'
            else:
                return 'Other'
                
    except Exception as e:
        return f'Invalid: {str(e)}'

def count_runs_with_croniter(cron_expression, start_time, end_time):
    """Count runs between start and end time using croniter"""
    cron = croniter(cron_expression, start_time)
    count = 0
    
    while True:
        try:
            next_run = cron.get_next(datetime)
            if next_run >= end_time:
                break
            count += 1
        except StopIteration:
            break
    
    return count

# COMMAND ----------

import re

def to_snake_case(name, source_origin):
    parts = name.split('/')

    if source_origin in ("SAP BW WCM", "DATA PORTAL"):
        return '/'.join(
            re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', part).lower()
            for part in parts
        )
    else:
        return '/'.join(
            re.sub(r'[^a-zA-Z0-9]+', '_',
                   re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', part)
                  ).strip('_').lower()
            for part in parts
        )


# COMMAND ----------

spark.sql(f"""
CREATE FUNCTION IF NOT EXISTS {catalog_name}.default.fn_CLEAN_SAP_STRING1(INPUT STRING)
RETURNS STRING
RETURN CASE
  WHEN LTRIM('0',INPUT) = '' THEN NULL
  ELSE LTRIM('0',INPUT)
END;
""")

# COMMAND ----------

spark.sql(f"""
CREATE FUNCTION IF NOT EXISTS {catalog_name}.default.FN_CLEAN_MOBILE_NO(P_MOBILE STRING)
RETURNS STRING
RETURN (
  CASE 
    WHEN P_MOBILE LIKE '840%' THEN SUBSTRING(P_MOBILE, 3, 15)
    WHEN P_MOBILE LIKE '.0%' THEN SUBSTRING(P_MOBILE, 2, 15)
    WHEN P_MOBILE LIKE '84%' AND LENGTH(P_MOBILE) > 10 THEN CONCAT('0', SUBSTRING(P_MOBILE, 3, 15))
    WHEN LEFT(P_MOBILE, 1) BETWEEN '1' AND '9' THEN CONCAT('0', P_MOBILE)
    ELSE P_MOBILE
  END
);
""")

# COMMAND ----------

proc_date = datetime.fromisoformat(proc_date).date()
print(proc_date)

# COMMAND ----------

import re

class SQLPatcher:
   def __init__(self, spark_instance, proc_date):
       self.original_sql = spark_instance.sql
       self.proc_date = proc_date
       
   def rewrite_sql(self, sql_query):
       # Replace current_timestamp() with proc_date at 00:00:00
       pattern1 = r'\bcurrent_timestamp\(\)'
       replacement1 = f"to_timestamp('{self.proc_date} 00:00:00')"
       sql_query = re.sub(pattern1, replacement1, sql_query, flags=re.IGNORECASE)
       
       # Replace current_date() with proc_date
       pattern2 = r'\bcurrent_date\(\)'
       replacement2 = f"to_date('{self.proc_date}')"
       sql_query = re.sub(pattern2, replacement2, sql_query, flags=re.IGNORECASE)

       pattern3 = r'\bnow\(\)'
       replacement3 = f"to_timestamp('{self.proc_date} 00:00:00')"
       sql_query = re.sub(pattern3, replacement3, sql_query, flags=re.IGNORECASE)

       pattern4 = r'\b current_timestamp(?=\s|$)'
       replacement4 = f" to_timestamp('{self.proc_date} 00:00:00')"
       sql_query = re.sub(pattern4, replacement4, sql_query, flags=re.IGNORECASE)

       pattern5 = r'\b current_date(?=\s|$)'
       replacement5 = f" to_date('{self.proc_date}') "
       sql_query = re.sub(pattern5, replacement5, sql_query, flags=re.IGNORECASE)

       return sql_query
   
   def patched_sql(self, sqlQuery,*args, **kwargs):
       rewritten_query = self.rewrite_sql(sqlQuery)
       return self.original_sql(rewritten_query,*args, **kwargs)

# Usage
patcher = SQLPatcher(spark, proc_date)
spark.sql = patcher.patched_sql

# COMMAND ----------

def create_temp_view_with_clean_columns(
    spark,
    catalog_name,
    schema_name,
    table_name,
    proc_date,
    temp_view_name
):
    """
    Create a temporary view with sanitized column names by replacing '/' with '_'.
    
    Edge Case:
        - If schema_name = 'udp_wcm_bronze_cx_loyalty', skip filtering by proc_date.

    Args:
        spark (SparkSession): The active Spark session.
        catalog_name (str): The catalog name.
        schema_name (str): The schema name.
        table_name (str): The table name.
        proc_date (str): The 'proc_date' to filter by.
        temp_view_name (str): The temp view name to create.

    Returns:
        None
    """

    # Step 1: Get all columns from the table
    column_list = spark.sql(
        f"DESCRIBE TABLE {catalog_name}.{schema_name}.{table_name}"
    ).collect()

    # Step 2: Build dynamic SELECT clause with sanitized column names
    select_columns = []
    for column in column_list:
        col_name = column[0]
        # Skip metadata rows
        if not col_name or col_name.startswith("#"):
            continue
        clean_col = col_name.replace('/', '_')
        select_columns.append(f"`{col_name}` AS {clean_col}")

    select_expr = ", ".join(select_columns)

    # Step 3: Handle edge case for skipping proc_date filter
    if schema_name == "udp_wcm_bronze_cx_loyalty":
        where_clause = ""
    else:
        where_clause = f" WHERE proc_date = DATE('{proc_date}')"

    # Step 4: Build and execute the dynamic query
    dynamic_query = f"""
        CREATE OR REPLACE TEMP VIEW {temp_view_name} AS
        SELECT {select_expr}
        FROM {catalog_name}.{schema_name}.{table_name}{where_clause}
    """
    
    spark.sql(dynamic_query)
    print(f"Temporary view '{temp_view_name}' created successfully!")

print('create_temp_view_with_clean_columns Function Created Successfully !!')

