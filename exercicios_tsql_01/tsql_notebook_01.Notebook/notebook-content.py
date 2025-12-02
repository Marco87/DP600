# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eabea36c-ce0f-4bb5-b1b2-30e042697ce1",
# META       "default_lakehouse_name": "tsql_lakehouse_01",
# META       "default_lakehouse_workspace_id": "8bafa015-7a97-4024-8b83-607a4fcc7c07",
# META       "known_lakehouses": [
# META         {
# META           "id": "eabea36c-ce0f-4bb5-b1b2-30e042697ce1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#df = spark.sql("SELECT * FROM tsql_lakehouse_01.dbo.sales LIMIT 1000")
df = spark.read.parquet("abfss://dp600@onelake.dfs.fabric.microsoft.com/tsql_lakehouse_01.Lakehouse/Tables/dbo/sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM tsql_lakehouse_01.dbo.sales LIMIT 1000")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
