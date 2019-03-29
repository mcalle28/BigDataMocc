# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('dates').getOrCreate()

# COMMAND ----------

df= sqlContext.sql('Select * from appl_stock_csv')

# COMMAND ----------

df.head(1)

# COMMAND ----------

from pyspark.sql.functions import dayofmonth,hour,dayofyear, month,year,weekofyear,format_number,date_format

# COMMAND ----------

df.select(dayofmonth(df['Date'])).show()

# COMMAND ----------

#df.withColumn('Year',year(df['Date'])).show()

# COMMAND ----------

newdf=df.withColumn('Year',year(df['Date']))

# COMMAND ----------

newdf.groupBy('Year').mean().show()

# COMMAND ----------

result=newdf.groupBy('Year').mean().select(['Year', "avg(Close)"])

# COMMAND ----------

new= result.withColumnRenamed("avg(Close)","Averange closing price")

# COMMAND ----------

new.select(['Year', format_number("Averange closing price",2).alias("Averange closing price")]).show()

# COMMAND ----------


