# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark= SparkSession.builder.appName('Basico').getOrCreate()

# COMMAND ----------

df= sqlContext.sql('Select * from people_json')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)

# COMMAND ----------

data_schema= [StructField('age',IntegerType(),True),StructField('name',StringType(),True)]

# COMMAND ----------

final_struc= StructType(fields=data_schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

type(df['age'])

# COMMAND ----------

df.select('age')

# COMMAND ----------

df.select('age').show()

# COMMAND ----------

df.head(2)

# COMMAND ----------

df.select(['age','name'])

# COMMAND ----------

df.select(['age','name']).show()


# COMMAND ----------

df.withColumn('double_age',df['age']*2).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.withColumnRenamed('age','mynewage')

# COMMAND ----------

df.createOrReplaceTempView('people')

# COMMAND ----------

results=spark.sql("SELECT * FROM people")

# COMMAND ----------

results.show()

# COMMAND ----------

new_sesults_= spark.sql("SELECT * FROM people WHERE age=30")

# COMMAND ----------

new_sesults_.show()

# COMMAND ----------


