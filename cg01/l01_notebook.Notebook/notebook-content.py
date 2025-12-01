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

#Carregando os dados da camada landing

df = spark.read.csv("Files/landing/sales.csv", header=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Fazendo a carga para a camada bronze

df.write.format("delta").mode("overwrite").save("Files/bronze/sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Fazendo a carga no DW

df.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
