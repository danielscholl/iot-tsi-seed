// Databricks notebook source
// MAGIC %md <h1>CSV to Parquet example</h1>
// MAGIC <p> This script defines the appropriate schema for the output Parquet files, reads the sample CSV from the input location using this schema, and writes the data back out to the output location as Parquet</p>

// COMMAND ----------

// Add the file and folder info in the "" below, then run this cell

// File source information
val storageAccountName = ""
val storageAccountAccessKey = ""
val containerName = "upload"
val blobPrefix = "convert"
val inputFileName = "sample.csv"

// Mount the folder
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net",
  mountPoint = "/mnt/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))


// COMMAND ----------

// Then run this cell to read in the csv using the defined schema, then display the data

// TimeStamp format:  yyy-MM-ddThh:mm:ss.fffZ

import org.apache.spark.sql.types._
sqlContext.setConf("spark.sql.parquet.outputTimestampType","TIMESTAMP_MILLIS")

// Define the source data schema, the below shows each of the valid types for use with TSI
// See the Bulk Upload Private Preview customer guide for more information
// This example requires the CSV columns to match the order of the schema

val schema = StructType(Array(
  StructField("timestamp", TimestampType, true),
  StructField("events_deviceId", StringType, true),
  StructField("value", DoubleType, true),
  StructField("questionable", BooleanType, true),
  StructField("substituted", BooleanType, true),
  StructField("annotated", BooleanType, true)))

val data = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .schema(schema)
  .load("/mnt/" + containerName + "/" + blobPrefix + "/" + inputFileName)

display(data)

// COMMAND ----------

// Finally, write the data back out in Parquet format
// The files will be located in the folder you defined, under a subfolder
// The file name is generated by spark, and will looks something like "part-xxxx...snappy.parquet"

data.write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet("/mnt/" + containerName + "/" + blobPrefix + "/parquet")


// COMMAND ----------

// To check this output file, provide the name and run this cell
val data2 = sqlContext.read.parquet("/mnt/" + containerName + "/" + blobPrefix + "/parquet")

display(data2)

// COMMAND ----------

// Clean up 

dbutils.fs.unmount("/mnt/" + containerName)
