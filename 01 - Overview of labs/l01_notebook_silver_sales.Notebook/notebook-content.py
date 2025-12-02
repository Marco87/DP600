# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6c4a64fd-b411-4895-9d2f-c8848d8e11e6",
# META       "default_lakehouse_name": "l01_lakehouse",
# META       "default_lakehouse_workspace_id": "8bafa015-7a97-4024-8b83-607a4fcc7c07",
# META       "known_lakehouses": [
# META         {
# META           "id": "6c4a64fd-b411-4895-9d2f-c8848d8e11e6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_bronze = spark.read.table("sales_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

df = df_bronze

for col in df.columns:
    df = df.withColumnRenamed(col, col.lower().replace(" ", "_"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df \
    .withColumn("salesorderlinenumber", F.col("salesorderlinenumber").cast("int")) \
    .withColumn("orderdate", F.to_date("orderdate", "yyyy-MM-dd")) \
    .withColumn("quantity", F.col("quantity").cast("int")) \
    .withColumn("unitprice", F.col("unitprice").cast("double")) \
    .withColumn("taxamount", F.col("taxamount").cast("double"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("processingtimestamp", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *df.columns), 256)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.dropDuplicates(["salesordernumber", "salesorderlinenumber"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.replace("", None)

df = df.fillna({
    "quantity": 0,
    "unitprice": 0.0,
    "taxamount": 0.0
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvando como silver

df.write.format("delta").mode("overwrite").save("Files/silver/sales_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_silver
    USING DELTA
    LOCATION 'Files/silver/sales_silver'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
