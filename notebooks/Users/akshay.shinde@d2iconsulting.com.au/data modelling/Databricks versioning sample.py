# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the Blob storages

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the Landing Zone Blob

# COMMAND ----------

# MAGIC %run Users/akshay.shinde@d2iconsulting.com.au/common_utilities/connect_landing_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the Silver Layer Blob

# COMMAND ----------

# MAGIC %run Users/akshay.shinde@d2iconsulting.com.au/common_utilities/connect_silver_layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the Golden Layer Blob

# COMMAND ----------

# MAGIC %run Users/akshay.shinde@d2iconsulting.com.au/common_utilities/connect_golden_layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import the Libraries

# COMMAND ----------

### PySpark ###
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import *

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

#### Changing the autoCompact to start considering compact after having 1 files. The default to start autocompact to work is 50 files.

spark.sql("set spark.databricks.delta.autoCompact.minNumFiles = 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the records from the Landing Zone

# COMMAND ----------

df = spark.read.option("header", "True").option("inferSchema", "True").csv("/mnt/landingzone/5mrecords.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform various transformations such as below for the Silver table:
# MAGIC * Having the abbrevations of the Order Priority
# MAGIC * Getting the Day from the Order Date

# COMMAND ----------

df_silver_sales_table_python =  df.withColumn('day_of_week',date_format(col("Order_Date"), "EEEE"))\
                                  .withColumn('Order_Priority',when(F.col('Order_Priority')=='H', 'High')\
                                                               .when(F.col('Order_Priority')=='M', 'Medium')\
                                                               .when(F.col('Order_Priority')=='L', 'Low')
                                                               .otherwise('Clear')\
                                                               )

# COMMAND ----------

display(df_silver_sales_table_python)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW landing_zone_table
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   path '/mnt/landingzone/5mrecords.csv',
# MAGIC   header 'true',
# MAGIC   inferSchema 'true'
# MAGIC );
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM landing_zone_table

# COMMAND ----------

df_silver_sales_table_sql = spark.sql("""
SELECT 
  *,
  date_format(Order_Date, 'EEEE') as day_of_week,
  CASE Order_Priority
    WHEN 'H' THEN 'High'
    WHEN 'M' THEN 'Medium'
    WHEN 'L' THEN 'Low'
    ELSE 'Clear'
  END as Order_Priority
FROM landing_zone_table
""")

# COMMAND ----------

display(df_silver_sales_table_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the final dataframe in the Silver layer blob location
# MAGIC * One can use the Python or the SQL dataframes to write the data in the silver layer (I will be using the Python dataframe)

# COMMAND ----------

silver_blob_location = "dbfs:/mnt/silver/sales_table/"

df_silver_sales_table_python.write.mode('append').format('delta').save(silver_blob_location)
spark.sql("CREATE TABLE IF NOT EXISTS silver_sales_fact USING DELTA LOCATION '" + silver_blob_location + "'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform various transformations such as below for the Silver table:
# MAGIC * Grouping the data by Country, Date and Day of week

# COMMAND ----------

df_gold_sales_table_python = spark.read.format("delta").load(silver_blob_location)\
                                       .groupBy('Order_Date','Country','day_of_week').agg(F.sum('Total_Revenue').alias('Total_Revenue'),F.sum('Total_Profit').alias('Total_Profit'))

# COMMAND ----------

display(df_gold_sales_table_python)

# COMMAND ----------

df_gold_sales_table_sql = spark.sql("""SELECT order_date,
                                        country,
                                        day_of_week,
                                        Sum(total_revenue) AS Total_Revenew,
                                        Sum(total_profit)  AS Total_Profit
                                    FROM   silver_sales_fact
                                    GROUP  BY order_date,
                                            country,
                                            day_of_week""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the final dataframe in the Golden layer blob location
# MAGIC * One can use the Python or the SQL dataframes to write the data in the silver layer (I will be using the Python dataframe)

# COMMAND ----------

gold_blob_location = "dbfs:/mnt/gold/sales_by_country/"

df_gold_sales_table_python.write.mode('overwrite').format('delta').save(gold_blob_location)
spark.sql("CREATE TABLE IF NOT EXISTS gold_sales_fact USING DELTA LOCATION '" + gold_blob_location + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE  gold_sales_fact
# MAGIC SET  Total_Revenue = round(Total_Revenue,2),
# MAGIC      Total_Profit =  round(Total_Profit,2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the version history

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY gold_sales_fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command used to check how many files are available for a written detla table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_fact