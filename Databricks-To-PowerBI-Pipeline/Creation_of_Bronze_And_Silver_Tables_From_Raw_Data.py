# Databricks notebook source
# MAGIC %md 
# MAGIC # Way's to use Databricks Notebook

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./Functions

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

sales_data_bronze    = rename_columns(sales_data_bronze)
order_data_bronze    = rename_columns(order_data_bronze)
customer_data_bronze = rename_columns(customer_data_bronze)
product_data_bronze  = rename_columns(product_data_bronze)

# COMMAND ----------

try:    
    sales_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.sales_data_silver")  

    order_data_bronze = (order_data_bronze.withColumn("shipping_mode",convertUDF("shipping_mode")))

    order_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.order_data_silver")  

    customer_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.customer_data_silver")

    product_data_bronze.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.product_data_silver")

    status = "Success"
except Exception as e:
  
    print(e)
    status = "Fail"

# COMMAND ----------

dbutils.notebook.exit(status)

# COMMAND ----------


