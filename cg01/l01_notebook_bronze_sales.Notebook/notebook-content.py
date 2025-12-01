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

# Lendo o arquivo .csv da camada landing

df_landing = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("Files/landing/sales.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mostrando o dataframe carregado

df_landing.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mostrando a schema do dataframe carregado

df_landing.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvando o dataframe gerado na camada bronze

df_landing.write.format("delta").mode("overwrite").save("Files/bronze/sales_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Carregando o arquivo da camada bronze no DW

spark.sql("DROP TABLE IF EXISTS sales_bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_bronze
    USING delta
    LOCATION 'Files/bronze/sales_bronze'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_landing.write.format("delta").mode("overwrite").saveAsTable("sales_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE sales_bronze AS
# MAGIC SELECT * FROM delta.`Files/bronze/sales_bronze`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
