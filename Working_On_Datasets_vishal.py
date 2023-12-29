# Databricks notebook source
pip install fuzzywuzzy

# COMMAND ----------

import pandas as pd 
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession
from fuzzywuzzy import fuzz

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Read .csv file from git repo

# COMMAND ----------


products = pd.read_csv(f"/Workspace/Repos/infy_163901@outlook.com/mslearn-databricks/data/products.csv")
products.display()

# COMMAND ----------

penguins = pd.read_csv(f"/Workspace/Repos/infy_163901@outlook.com/mslearn-databricks/data/penguins.csv")
penguins.display()

# COMMAND ----------

df_2021 = pd.read_csv(f"/Workspace/Repos/infy_163901@outlook.com/mslearn-databricks/data/2021.csv")
df_2021.display()

# COMMAND ----------

# MAGIC %md
# MAGIC * Convert pandas dataframe to spark dataframe

# COMMAND ----------

df_2021  = spark.createDataFrame(df_2021)
products = spark.createDataFrame(products)
penguins = spark.createDataFrame(penguins)

# COMMAND ----------

products.limit(10).display()

# COMMAND ----------

penguins.limit(10).display()

# COMMAND ----------

df_2021.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### playing with cretaed spark dataframes
# MAGIC
# MAGIC * products
# MAGIC * penguins
# MAGIC * df_2021

# COMMAND ----------

products.columns

# COMMAND ----------

products_col_renamed = (products.withColumnRenamed('ProductID','Product_Id')
                   .withColumnRenamed('ProductName','Product_Name')
                   .withColumnRenamed('Category','Categorys')
                   .withColumnRenamed('ListPrice','List_Price')  
          )
products_col_renamed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How to use sql in spark to access database tables
# MAGIC
# MAGIC * default is a database
# MAGIC * circuits is a csv

# COMMAND ----------

circuits = spark.sql(""" select * from default.circuits""")
diabetes = spark.sql(""" select * from default.diabetes """)

# COMMAND ----------

circuits.limit(10).display()

# COMMAND ----------

diabetes.limit(10).display()

# COMMAND ----------

circuits = spark.table("default.circuits")
circuits.write.format("delta").mode('overwrite')

#---------------------

diabetes = spark.table("default.diabetes")
diabetes.write.format("delta").mode('overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Playing around with pyspark 
# MAGIC * use of concat
# MAGIC * use of regexp_replace
# MAGIC * use of filter

# COMMAND ----------

circuits = (circuits.withColumn('location_country',
                                f.concat(f.col('location'),f.lit(' , '),f.col('country'))) 
           )
circuits.display()

# COMMAND ----------

circuits = (circuits.withColumn('countrys', regexp_replace('country', 'Australia', '0')))
circuits.filter(f.col("countrys")=="0").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDF 

# COMMAND ----------

def convertFirstLToLowerCase(str):
    resStr=""
    arr = str.split(" ")
    if str == "NA":
        return "NA"
    else:
        for x in arr:
            resStr= resStr + x[0:1].lower() + x[1:len(x)]
        return resStr 
convertUDF = udf(lambda z: convertFirstLToLowerCase(z),StringType())

# COMMAND ----------

circuits = (circuits.withColumn("countrys_lower", convertUDF(col("country"))) )
circuits.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fuzzywuzzy String Match

# COMMAND ----------


def generate_string_ratio(str1, str2):
  if None is str1 or None is str2:
    return None
  return { 
    "ratio"            : fuzz.ratio(str1.lower(),str2.lower()),
    "partial_ratio"    : fuzz.partial_ratio(str1.lower(),str2.lower()),
    "token_ratio"      : fuzz.token_sort_ratio(str1,str2),
    "token_set_ratio"  : fuzz.token_set_ratio(str1,str2) 
  }

# COMMAND ----------

generate_string_ratio("vishal.mariya","vi.mariy")

# COMMAND ----------


