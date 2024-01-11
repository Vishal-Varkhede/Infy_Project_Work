# Databricks notebook source
# MAGIC %md 
# MAGIC # Way's to use Databricks Notebook

# COMMAND ----------

# MAGIC %run ./Functions/Functions

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

# Consider a table named sales with the following columns: sale_date (date of sale), product_id (unique identifier for each product), and revenue (amount of revenue generated from the sale).

# COMMAND ----------

# Write an spark query that calculates the cumulative revenue for each product over time, ordered by the sale date. The result should include the sale date, product ID, revenue for that day, and the cumulative revenue up to that day for each product.

# The output should look like this:

# Expected Output:

# +------------+------------+--------+----------------+

# | sale_date  | product_id | revenue| cumulative_revenue|

# +------------+------------+--------+----------------+

# | 2023-01-01 | 1          | 100.00 | 100.00          |

# | 2023-01-01 | 2          | 150.00 | 150.00          |

# | 2023-01-02 | 1          | 120.00 | 220.00          |

# | 2023-01-02 | 2          | 180.00 | 330.00          |

# | 2023-01-03 | 1          | 90.00  | 310.00          |

# | 2023-01-03 | 2          | 200.00 | 530.00          |

# +------------+------------+--------+----------------+

# In this example, cumulative_revenue represents the total revenue generated by each product up to the corresponding sale date.
# Note :- The expectation is that the output of the sql query is in a table and that table should be converted into the spark dataframe.


# COMMAND ----------


