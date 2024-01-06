# Databricks notebook source
# MAGIC %run ./Functions

# COMMAND ----------

sales_data_silver    = spark.sql("select * from default.sales_data_silver")
order_data_silver    = spark.sql("select * from default.order_data_silver")
customer_data_silver = spark.sql("select * from default.customer_data_silver")
product_data_silver  = spark.sql("select * from default.product_data_silver")

# COMMAND ----------



# COMMAND ----------

try:
    # sales_data_silver.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.sales_data_gold")  

    # order_data_silver.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.order_data_gold")  

    # customer_data_silver.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.customer_data_gold")

    # product_data_silver.write.format("delta").option("mergeSchema", "true").mode('overwrite').saveAsTable("default.product_data_gold")
    status = "Success"
except Exception as e:
  
    print(e)
    status = "Fail"

# COMMAND ----------

dbutils.notebook.exit(status)
