# Databricks notebook source
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

scc= StreamingContext(sc,1)

# COMMAND ----------

lines= scc.socketTextStream('localhost',9999)

# COMMAND ----------

words= lines.flatMap(lambda line: line.split(' '))

# COMMAND ----------

pairs= words.map(lambda word: (word,1))

# COMMAND ----------

word_count= pairs.reduceByKey(lambda num1, num2: num1+num2 )

# COMMAND ----------

word_count.pprint()

# COMMAND ----------

scc.start()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh -e nc -lk 9999

# COMMAND ----------


