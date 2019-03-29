# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark= SparkSession.builder.appName('aggs').getOrCreate()

# COMMAND ----------

df= sqlContext.sql('Select * from sales_info_csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.groupBy("Company")

# COMMAND ----------

df.groupBy('Company').mean().show()

# COMMAND ----------

df.agg({'Sales':'sum'}).show()

# COMMAND ----------

group_data=df.groupBy('Company')

# COMMAND ----------

group_data.agg({'Sales':'max'}).show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct,avg,stddev

# COMMAND ----------

df.select(countDistinct('Sales')).show()

# COMMAND ----------

df.select(avg('Sales')).show()

# COMMAND ----------

df.select(stddev('Sales')).show()

# COMMAND ----------

from pyspark.sql.functions import format_number

# COMMAND ----------

sales_std= df.select(stddev('Sales').alias('std'))

# COMMAND ----------

sales_std.select(format_number('std',2)).show()

# COMMAND ----------

df.orderBy('Sales').show()

# COMMAND ----------

df.orderBy(df['Sales'].desc()).show()

# COMMAND ----------


