// Databricks notebook source
// MAGIC %md <h1>Read Parquet Blob</h1>
// MAGIC <p> This script reads a parquet blob</p>

// COMMAND ----------

// Add the file and folder info in the "" below, then run this cell

// File source information
val storageAccountName = ""
val storageAccountAccessKey = ""
val containerName = "conversion"
val blobPrefix = "convert"
val inputFileName = "sample.parquet"

// Mount the folder
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net",
  mountPoint = "/mnt/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))

// COMMAND ----------

// To check this output file, provide the name and run this cell
val data = sqlContext.read.parquet("/mnt/" + containerName + "/" + blobPrefix + "/" + inputFileName)

display(data)

// COMMAND ----------

// Clean up 
val containerName = "upload"

dbutils.fs.unmount("/mnt/" + containerName)
