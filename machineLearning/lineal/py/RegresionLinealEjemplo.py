# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('ejemplo_rl').getOrCreate()
from pyspark.ml.regression import LinearRegression

# COMMAND ----------

data = sqlContext.sql('Select * from ecommerce_customers_83b1c_csv')

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data.show()

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

data.columns

# COMMAND ----------

assembler= VectorAssembler(inputCols=['Avg Session Length','Time on App','Time on Website', 'Length of Membership'],outputCol='features')

# COMMAND ----------

output= assembler.transform(data)

# COMMAND ----------

output.printSchema()

# COMMAND ----------

final_data= output.select('features','Yearly Amount Spent')

# COMMAND ----------

final_data.show()

# COMMAND ----------

train_data,test_data= final_data.randomSplit([0.7,0.3])

# COMMAND ----------

lr= LinearRegression(labelCol='Yearly Amount Spent')

# COMMAND ----------

lr_model=lr.fit(train_data)

# COMMAND ----------

test_results=lr_model.evaluate(test_data)

# COMMAND ----------

test_results.residuals.show()

# COMMAND ----------

test_results.rootMeanSquaredError

# COMMAND ----------

test_results.r2

# COMMAND ----------

unlabeled_data= test_data.select('features')

# COMMAND ----------

unlabeled_data.show()

# COMMAND ----------

predictions=lr_model.transform(unlabeled_data)

# COMMAND ----------

predictions.show()

# COMMAND ----------

#fin

# COMMAND ----------


