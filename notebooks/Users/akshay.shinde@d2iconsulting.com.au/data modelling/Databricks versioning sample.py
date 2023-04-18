# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the Blob storages

# COMMAND ----------

# MAGIC %run Users/akshay.shinde@d2iconsulting.com.au/common_utilities/connect_landing_zone

# COMMAND ----------

# MAGIC %run Users/akshay.shinde@d2iconsulting.com.au/common_utilities/connect_bronze_layer

# COMMAND ----------

pip install quinn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import the Libraries

# COMMAND ----------

### PySpark ###
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import last
from pyspark.sql.functions import expr
from dateutil.relativedelta import relativedelta, MO
### Quinn ###
import quinn as q
from quinn.extensions import * 
### Other ###
import datetime as dt
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## Making sure the Delta write is optimised

# COMMAND ----------

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## I have many small files. Why is auto optimize not compacting them?
# MAGIC By default, auto optimize does not begin compacting until it finds more than 50 small files in a directory. You can change this behavior by setting
# MAGIC * spark.databricks.delta.autoCompact.minNumFiles.
# MAGIC 
# MAGIC  Having many small files is not always a problem, since it can lead to better data skipping, and it can help minimize rewrites during merges and deletes. However, having too many small files might be a sign that your data is over-partitioned.

# COMMAND ----------

#### Changing the autoCompact to start considering compact after having 2 files. The default to start autocompact to work is 50 files.

spark.sql("set spark.databricks.delta.autoCompact.minNumFiles = 2")

# COMMAND ----------

df = spark.read.option("header", "True").option("inferSchema", "True").csv("/mnt/landingzone/5mrecords.csv")

# COMMAND ----------

bronze_location = "dbfs:/mnt/bronze/sales_fact/"

df.write.mode('append').format('delta').save(bronze_location)

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS sales_fact USING DELTA LOCATION '" + bronze_location + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM sales_fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the version history

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY sales_fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update the Sales Fact
# MAGIC 
# MAGIC * The version 1 consists of an update that we did wherein the below sql statement was executed
# MAGIC * UPDATE sales_fact SET Order_Priority = 'High' WHERE Order_Priority = 'H'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE sales_fact
# MAGIC SET Order_Priority = 'High' WHERE Order_Priority = 'H';
# MAGIC 
# MAGIC UPDATE sales_fact
# MAGIC SET Order_Priority = 'Medium' WHERE Order_Priority = 'M';
# MAGIC 
# MAGIC UPDATE sales_fact
# MAGIC SET Order_Priority = 'Low' WHERE Order_Priority = 'L';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Repeat the update till we reach version 31

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command used to check how many files are available for a written detla table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ignore the below cells

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# spark.sql("VACUUM sales_fact RETAIN 168 HOURS")