# Databricks notebook source
#Start a simple Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("walmart").getOrCreate()

# COMMAND ----------

#Load the Walmart Stock CSV File, have Spark infer the data types
# Este punto, se realiza además con la carga de datos por la interfaz visual de databricks
df= sqlContext.sql('Select * from walmart_stock_csv')

# COMMAND ----------

#What are the column names?
df.columns

# COMMAND ----------

#What does the Schema look like?
df.printSchema()

# COMMAND ----------

#Print out the first 5 columns
for row in df.head(5):
  print(row)
  print('\n')

# COMMAND ----------

#Use describe() to learn about the DataFrame
df.describe().show()

# COMMAND ----------

#There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar. [Check this link for a hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast)

#df.describe().printSchema()
from pyspark.sql.functions import format_number
resultd=df.describe()
resultd.select(resultd['summary'],format_number(resultd['Open'].cast('float'),2).alias('Open'),
               format_number(resultd['High'].cast('float'),2).alias('High'),
               format_number(resultd['Low'].cast('float'),2).alias('Low'),
               format_number(resultd['Close'].cast('float'),2).alias('Close'),
              resultd['Volume'].cast('int').alias('Volume')).show()

# COMMAND ----------

#create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.
df2 = df.withColumn("HV Ratio",df["High"]/df["Volume"])
df2.select('HV Ratio').show()

# COMMAND ----------

#What day had the Peak High in Price?
df.orderBy(df["High"].desc()).head(1)[0][0]

# COMMAND ----------

#What is the mean of the Close column?
from pyspark.sql.functions import mean
df.select(mean('Close')).show()

# COMMAND ----------

#What is the max and min of the Volume column?
from pyspark.sql.functions import max, min
df.select(max("Volume"),min("Volume")).show()

# COMMAND ----------

#How many days was the Close lower than 60 dollars?
df.filter('Close<60').count()

# COMMAND ----------

#What percentage of the time was the High greater than 80 dollars
(df.filter('High>80').count()*1.0/df.count())*100

# COMMAND ----------

#What is the Pearson correlation between High and Volume?
from pyspark.sql.functions import corr
df.select(corr('High','Volume')).show()

# COMMAND ----------

#What is the max High per year?
from pyspark.sql.functions import year

yeardf=df.withColumn('Year',year(df['Date']))
max_yf= yeardf.groupBy('Year').max()
max_yf.select('Year', 'max(High)').show()

# COMMAND ----------

#What is the average Close for each Calendar Month?
from pyspark.sql.functions import month
monthdf = df.withColumn("Month",month("Date"))
monthavgs = monthdf.select("Month","Close").groupBy("Month").mean()
monthavgs.select("Month","avg(Close)").orderBy('Month').show()

# COMMAND ----------


