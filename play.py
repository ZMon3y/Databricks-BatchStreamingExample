# Databricks notebook source
# DBTITLE 1,Explore Public Datasets
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# DBTITLE 1,Investigate IOT Stream
display(dbutils.fs.ls('/databricks-datasets/iot-stream/'))

# COMMAND ----------

# DBTITLE 1,IOT Stream README.md
f = open('/dbfs/databricks-datasets/iot-stream/README.md', 'r')
print(f.read())

# COMMAND ----------

# DBTITLE 1,IOT Stream Device Files
display(dbutils.fs.ls('/databricks-datasets/iot-stream/data-device/'))

# COMMAND ----------

# DBTITLE 1,IOT Stream User File
display(dbutils.fs.ls('/databricks-datasets/iot-stream/data-user/'))

# COMMAND ----------

# DBTITLE 1,Look Inside the CSV
print(open('/dbfs/databricks-datasets/iot-stream/data-user/userData.csv', 'r').read())
# print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC # Device Data Investigation
# MAGIC ![investigation](https://vertassets.blob.core.windows.net/image/5a8ec45f/5a8ec45f-31cd-4913-97b8-7e08e51e561e/375_250-investigation_magnifying_glass_thinkstockphotos_473800928.png)

# COMMAND ----------

# Import libraries for SQL Types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

# Define Schema
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

# COMMAND ----------

# Create Device DataFrame
dataDevice_df = spark.read.schema(dataDeviceSchema).json('dbfs:/databricks-datasets/iot-stream/data-device/')

# COMMAND ----------

display(dataDevice_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # User Data Investigation

# COMMAND ----------

# Define Schema
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

# Create User DataFrame
user_df = spark.read.schema(userSchema).csv('/databricks-datasets/iot-stream/data-user/', header="true")

# COMMAND ----------

display(user_df)

# COMMAND ----------

joined_df = dataDevice_df.join(user_df, dataDevice_df.user_id == user_df.userid).drop(dataDevice_df.user_id).drop('value')

# COMMAND ----------

joined_df.display()

# COMMAND ----------

joined_df.createOrReplaceTempView('joined_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(distinct userid)
# MAGIC FROM joined_df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT userid, count(distinct device_id)
# MAGIC FROM joined_df
# MAGIC GROUP BY userid 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT smoker, gender, approx_count_distinct(userid) as numUsers, AVG(num_steps) as avgNumSteps, AVG(miles_walked) as avgMilesWalked, AVG(calories_burnt) as avgCaloriesBurnt
# MAGIC FROM joined_df
# MAGIC GROUP BY smoker, gender
# MAGIC ORDER BY smoker, gender

# COMMAND ----------

smokerAgg_df = spark.sql("""
    SELECT smoker, gender, approx_count_distinct(userid) as numUsers, AVG(num_steps) as avgNumSteps, AVG(miles_walked) as avgMilesWalked, AVG(calories_burnt) as avgCaloriesBurnt
    FROM joined_df
    GROUP BY smoker, gender
    ORDER BY smoker, gender
""")