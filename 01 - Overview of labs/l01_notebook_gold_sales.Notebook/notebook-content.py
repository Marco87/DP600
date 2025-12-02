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

df_silver = spark.read.table("sales_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Criando a dimensão customer

# CELL ********************

import pyspark.sql.functions as F

dim_customer = (
    df_silver
    .select("customername", "emailaddress")
    .dropDuplicates()
    .withColumn("idcustomer", F.monotonically_increasing_id())
)

dim_customer = dim_customer.select("idcustomer", "customername", "emailaddress")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer.write.format("delta").mode("overwrite").save("Files/gold/dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_customer
    USING DELTA
    LOCATION 'Files/gold/dim_customer'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Criando a dimensão product

# CELL ********************

dim_product = (
    df_silver
    .select("item")
    .dropDuplicates()
    .withColumn("idproduct", F.monotonically_increasing_id())
)

dim_product = dim_product.select("idproduct", "item")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product.write.format("delta").mode("overwrite").save("Files/gold/dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_product
    USING DELTA
    LOCATION 'Files/gold/dim_product'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Criando a dimensão data

# CELL ********************

from pyspark.sql import Window

df_dates = df_silver.select("orderdate").dropDuplicates()

dim_date = (
    df_dates
    .withColumn("iddate", F.row_number().over(Window.orderBy("orderdate")))
    .withColumn("year", F.year("orderdate"))
    .withColumn("month", F.month("orderdate"))
    .withColumn("day", F.dayofmonth("orderdate"))
    .withColumn("quarter", F.quarter("orderdate"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date.write.format("delta").mode("overwrite").save("Files/gold/dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_date
    USING DELTA
    LOCATION 'Files/gold/dim_date'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Criando a fato vendas

# CELL ********************

fact_sales = (
    df_silver.alias("f")
    .join(dim_customer.alias("c"), "emailaddress")
    .join(dim_product.alias("p"), "item")
    .join(dim_date.alias("d"), "orderdate")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales = fact_sales.select(
    "salesordernumber",
    "salesorderlinenumber",
    "idcustomer",
    "idproduct",
    "iddate",
    "quantity",
    "unitprice",
    "taxamount",
    (F.col("quantity") * F.col("unitprice")).alias("salesamount"),
    "processingtimestamp",
    "row_hash"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales.write.format("delta").mode("overwrite").save("Files/gold/fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_sales
    USING DELTA
    LOCATION 'Files/gold/fact_sales'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
