# Databricks notebook source
# DBTITLE 1,Initialize Widgets
dbutils.widgets.text('isStreaming','False')
dbutils.widgets.text('sourceData', 'dbfs:/databricks-datasets/iot-stream/')

sourceData = dbutils.widgets.get('sourceData')

if dbutils.widgets.get('isStreaming') == "False":
    isStreaming = False
else: 
    isStreaming = True

# COMMAND ----------

# DBTITLE 1,Define Schemas
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, FloatType

# Device Schema
dataDeviceSchema = StructType([
    StructField("id",LongType(),False),  
    StructField("user_id",LongType(),True),  
    StructField("device_id",LongType(),True),  
    StructField("num_steps",LongType(),True),  
    StructField("miles_walked",FloatType(),True),  
    StructField("calories_burnt",FloatType(),True),  
    StructField("timestamp",StringType(),True),  
    StructField("value",StringType(),True)
])

# User Schema
userSchema = StructType([
    StructField("userid",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("height",IntegerType(),True),
    StructField("weight",IntegerType(),True),
    StructField("smoker",StringType(),True),
    StructField("familyhistory",StringType(),True),
    StructField("cholestlevs",StringType(),True),
    StructField("bp",StringType(),True),
    StructField("risk",IntegerType(),True)
])

# COMMAND ----------

# DBTITLE 1,Define Source DataFrames
if isStreaming:
    dataDevice_df = spark.readStream.schema(dataDeviceSchema).json(sourceData + 'data-device/')
else:
    dataDevice_df = spark.read.schema(dataDeviceSchema).json(sourceData + 'data-device/')
    
user_df = spark.read.schema(userSchema).csv(sourceData + 'data-user/', header="true")

# COMMAND ----------

# DBTITLE 1,Define Joined DataFrame
joined_df = dataDevice_df.join(user_df, dataDevice_df.user_id == user_df.userid).drop(dataDevice_df.user_id).drop('value')
joined_df.createOrReplaceTempView('joined_df')

# COMMAND ----------

# DBTITLE 1,Define Aggregate DataFrame
smokerAgg_df = spark.sql("""
    SELECT smoker, gender, approx_count_distinct(userid) as numUsers, AVG(num_steps) as avgNumSteps, AVG(miles_walked) as avgMilesWalked, AVG(calories_burnt) as avgCaloriesBurnt
    FROM joined_df
    GROUP BY smoker, gender 
    ORDER BY smoker, gender
""")

# COMMAND ----------

# DBTITLE 1,Save Table
if isStreaming:
    smokerAgg_df.writeStream.format("delta").outputMode("complete").option("checkpointLocation", "/mnt/delta/eventsByCustomer/_checkpoints/streaming-agg").start("default.smokerAgg")
else:
    smokerAgg_df.write.format("delta").mode("overwrite").saveAsTable("default.smokerAgg")