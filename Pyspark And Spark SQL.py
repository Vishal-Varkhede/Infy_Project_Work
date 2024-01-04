# Databricks notebook source
# MAGIC %md 
# MAGIC # Way's to use Databricks Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importing library's

# COMMAND ----------

pip install fuzzywuzzy

# COMMAND ----------

dbutils.library.restartPython()

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

# MAGIC %md
# MAGIC ##### Python Code

# COMMAND ----------

print("Hello")

# COMMAND ----------

def addition(a,b):
    
    c = a + b
    
    return c

addition(10,20)

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

# MAGIC %md
# MAGIC
# MAGIC * Overview of Data Lake Storage 
# MAGIC A data lake is a central repository that allows companies to store all their structured and unstructured data at any scale. The data is stored in its raw format, without pre-processing or modeling, making it easily accessible for various use cases such as big data analytics, data science, and machine learning. Data lakes can be built on multiple technologies, such as Hadoop, Amazon S3, and Azure Data Lake Storage.
# MAGIC
# MAGIC * Introduction to Delta Tables
# MAGIC Delta Lake Delta tables are a new feature in Apache Spark that provides a more efficient way to store and manage large-scale data lakes. They are built on top of the existing Parquet format and provide several benefits over traditional data lake storage. The core technology behind Delta tables is called Delta Lake, an open-source storage format that brings reliability and performance to data lakes.
# MAGIC
# MAGIC * Benefits of Using Delta Tables 
# MAGIC Delta tables provide several benefits over traditional data lake storage, such as:
# MAGIC
# MAGIC     1: Improved performance: Delta tables use a new file format called Delta Lake that is optimized for performance, especially when dealing with large amounts of data.
# MAGIC
# MAGIC     2: Data versioning: Delta tables automatically keep track of all changes to the data, making it easy to revert to previous versions or see a history of changes.
# MAGIC
# MAGIC     3: ACID transactions: Delta tables provide support for ACID (Atomicity, Consistency, Isolation, Durability) transactions, allowing multiple users to update the same table without conflicts.
# MAGIC
# MAGIC     4: Schema enforcement: Delta tables enforce a schema on the data, ensuring that all data is consistent and can be queried efficiently.
# MAGIC
# MAGIC     5: Time travel: Allows to query data as it existed at any time, enabling easy data lineage and auditing.
# MAGIC     Optimistic concurrency control: allows multiple users to read and write a table simultaneously, without conflicts.

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### How to covert csv file to delta

# COMMAND ----------

# MAGIC %md
# MAGIC * My understanding from the documentation is that if I have multiple parquet partitions with different schemas, spark will be able to merge these schemas automatically if I use spark.read.option("mergeSchema", "true").parquet(path).
# MAGIC
# MAGIC * This seems like a good option if I don't know at query time what schemas exist in these partitions.
# MAGIC
# MAGIC * However, consider the case where I have two partitions, one using an old schema, and one using a new schema that differs only in having one additional field. Let's also assume that my code knows the new schema and I'm able to pass this schema in explicitly.
# MAGIC
# MAGIC * Use it if you want to handle chema evolution (changes to the schema over time).
# MAGIC
# MAGIC * If you specifically need to manage schema evolution explicitly when writing data to dalta table,you can use the
# MAGIC .option("mergeSchema","True")

# COMMAND ----------

circuits = spark.table("default.circuits")
circuits.write.format("delta").option("mergeSchema", "true").mode('overwrite')

#---------------------

diabetes = spark.table("default.diabetes")
diabetes.write.format("delta").option("mergeSchema", "true").mode('overwrite')


# COMMAND ----------

# MAGIC %md 
# MAGIC * When creating a delta table in Spark,specifying the schema is usually no necessary as Spark can infer it from the DataFrame.

# COMMAND ----------

circuits = spark.table("default.circuits")
circuits.write.format("delta").mode('overwrite')

#---------------------

diabetes = spark.table("default.diabetes")
diabetes.write.format("delta").mode('overwrite')

# COMMAND ----------

circuits.display()

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

def replace(value):
    return "NA" if value is None or value == "0" else value

replace = udf(replace,StringType())

# COMMAND ----------

circuits = (circuits.withColumn("cont", replace(circuits["countrys"])))
circuits.display()

# COMMAND ----------

circuits = (circuits.withColumn("countrys", convertUDF(col("cont"))) )
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

generate_string_ratio("vishal","ViSHa")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Plaing around with Spark SQL

# COMMAND ----------

circuits = spark.sql(""" select * from default.circuits""")
diabetes = spark.sql(""" select * from default.diabetes """)

# COMMAND ----------

circuits.limit(10).display()

# COMMAND ----------

# dbutils.widgets.text("namee", "Brickster", "Name")
# dbutils.widgets.multiselect("colorss", "orange", ["red", "orange", "black", "blue"], "Traffic Sources")

# COMMAND ----------

name = dbutils.widgets.get("name")
colors = dbutils.widgets.get("colors").split(",")

html = "<div>Hi {}! Select your color preference.</div>".format(name)
for c in colors:
  html += """<label for="{}" style="color:{}"><input type="radio"> {}</label><br>""".format(c, c, c)

displayHTML(html)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### File system access commands

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/COVID

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/COVID/CORD-19/.DS_Store")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How to upload and read image data in databricks
# MAGIC
# MAGIC * Used "dbutils.fs.ls" to locate the file

# COMMAND ----------

dbutils.fs.ls(f"/FileStore/tables/okj.jpg")

# COMMAND ----------

sample_img_dir = f"/FileStore/tables/okj.jpg"

Image_df = spark.read.format("image").load(sample_img_dir)

display(Image_df) 

# COMMAND ----------

print("hello")

# COMMAND ----------


