# Databricks notebook source
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

def __rename_df_col(df, columMapping):
  for re_col in columMapping:
    df = df.withColumnRenamed(re_col, columMapping.get(re_col))
  return df

def rename_columns(df):

  columns_map = {
   "Order_ID"                        : 'order_id'
  ,"Order_Date"                      : "order_date"
  ,"Ship_Date"                       : "shipping_date"
  ,"Customer_ID"                     : "customers_id"
  ,"Product_ID"                      : "product_id"
  ,"Quantity"                        : "order_quantity"
  ,"Discount"                        : "order_discount"
  ,"Profit"                          : "order_profit"
  ,"Ship_Mode"                       : "shipping_mode"
  ,"Customer_Name"                   : "customers_name"
  ,"Postal_Code"                     : "postal_code"
  ,"Sub-Category"                    : "sub_category"
  ,"Product_Name"                    : "product_name"
  ,"Sales"                           : "sales"
  ,"Segment"                         : "segment"
  ,"Country"                         : "country"
  ,"City"                            : "city"
  ,"State"                           : "state"
  ,"Region"                          : "region"
  ,"Category"                        : "category"
  }
  
  renamed      = __rename_df_col(df,columns_map)
  
  return renamed

# COMMAND ----------

def convertFirstLToLowerCase(str):
    resStr=""
    arr = str.split(" ")
    if str == "NA":
        return "NA"
    else:
        arr = str.split(" ")
        resStr= " ".join([x[0].lower() + x[1:] for x in arr])
        return resStr 


convertUDF = udf(lambda z: convertFirstLToLowerCase(z),StringType())
spark.udf.register("convertUDF", convertUDF)


# COMMAND ----------

def replace_str(value):
    
    return "NA" if value is None or value == "0" else value

replace_udf = udf(replace_str,StringType())
spark.udf.register("replace", replace_udf)

# COMMAND ----------

def original_str(value):
    
    return "Australia" if value == "NA" or value == "0" else value

original_str_udf = udf(original_str,StringType())
spark.udf.register("replace", original_str_udf)

# COMMAND ----------



# COMMAND ----------


