# Databricks notebook source
# DBTITLE 1,Libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

# COMMAND ----------

# DBTITLE 1,Define Schemas
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
dataDevice_df = spark.read.schema(dataDeviceSchema).json('dbfs:/databricks-datasets/iot-stream/data-device/')
user_df = spark.read.schema(userSchema).csv('/databricks-datasets/iot-stream/data-user/', header="true")

# COMMAND ----------

# DBTITLE 1,Define Joined DataFrame
joined_df = dataDevice_df.join(user_df, dataDevice_df.user_id == user_df.userid).drop(dataDevice_df.user_id).drop('value')
joined_df.createOrReplaceTempView('joined_df')

# COMMAND ----------

# DBTITLE 1,Define Aggregate DataFrame
smokerAgg_df = spark.sql("""
    SELECT smoker, gender, approx_count_distinct(distinct userid) as numUsers, AVG(num_steps) as avgNumSteps, AVG(miles_walked) as avgMilesWalked, AVG(calories_burnt) as avgCaloriesBurnt
    FROM joined_df
    GROUP BY smoker, gender
    ORDER BY smoker, gender
""")

# COMMAND ----------

# DBTITLE 1,Save Table
smokerAgg_df.write.format("delta").mode("overwrite").saveAsTable("default.smokerAgg")