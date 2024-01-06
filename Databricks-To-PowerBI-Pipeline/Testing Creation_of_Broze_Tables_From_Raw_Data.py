# Databricks notebook source
# MAGIC %md 
# MAGIC # Way's to use Databricks Notebook

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importing library's

# COMMAND ----------

# Import needed libraries
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from fuzzywuzzy import fuzz

# COMMAND ----------

sales_data = spark.sql(""" select * from default.sales_data_csv""")
order_data = spark.sql(""" select * from default.order_data_csv""")
customer_data = spark.sql(""" select * from default.customer_data_csv""")
product_data = spark.sql(""" select * from default.product_data_csv""")

# COMMAND ----------

sales_data.write.format("delta").option("mergeSchema","True").mode("overwrite").saveAsTable("default.sales_data_bronze")

order_data.write.format("delta").option("mergeSchema","True").mode("overwrite").saveAsTable("default.order_data_bronze")

customer_data.write.format("delta").option("mergeSchema","True").mode("overwrite").saveAsTable("default.customer_data_bronze")

product_data.write.format("delta").option("mergeSchema","True").mode("overwrite").saveAsTable("default.product_data_bronze")

# COMMAND ----------

sales_data_bronze = spark.sql(""" select * from default.sales_data_bronze """)
order_data_bronze = spark.sql(""" select * from default.order_data_bronze """)
customer_data_bronze = spark.sql(""" select * from default.customer_data_bronze """)
product_data_bronze = spark.sql(""" select * from default.product_data_bronze """)

# COMMAND ----------

order_data_bronze.display()

# COMMAND ----------

sales_data_bronze    = rename_columns(sales_data_bronze)
order_data_bronze    = rename_columns(order_data_bronze)
customer_data_bronze = rename_columns(customer_data_bronze)
product_data_bronze  = rename_columns(product_data_bronze)

# COMMAND ----------

sales_data_bronze.display()

# COMMAND ----------

sales_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.sales_data_silver")  

# COMMAND ----------

order_data_bronze.display()

# COMMAND ----------

order_data_bronze = (order_data_bronze.withColumn("shipping_mode",convertUDF("shipping_mode")))

order_data_bronze.display()

# COMMAND ----------

order_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.order_data_silver")  

# COMMAND ----------

customer_data_bronze.display()

# COMMAND ----------

customer_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.customer_data_silver")

# COMMAND ----------

product_data_bronze.display()

# COMMAND ----------

product_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.product_data_silver")

# COMMAND ----------

sales_data_silver = spark.sql("select * from default.sales_data_silver")
order_data_silver = spark.sql("select * from default.order_data_silver")
customer_data_silver = spark.sql("select * from default.customer_data_silver")
product_data_silver = spark.sql("select * from default.product_data_silver")

# COMMAND ----------

sales_data_silver.display()

# COMMAND ----------

order_data_silver.display()

# COMMAND ----------

customer_data_silver.display()

# COMMAND ----------

product_data_silver.display()

# COMMAND ----------

# dbutils.notebook.exit(status)

# COMMAND ----------


