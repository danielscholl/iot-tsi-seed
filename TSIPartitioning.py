# Databricks notebook source
# MAGIC %md <h1>TSI Data Partitioning for historical data</h1>
# MAGIC <p> This script reads parquet files from input location, partitions the data to the requirement of TSI and writes them to the output location</p>

# COMMAND ----------

import json

# Input files source information
inputStorageAccountName = ""
inputStorageAccountAccessKey = ""
inputContainerName = ""
# folder path inside conatiner
inputBlobPrefix = ""

# Dont edit this
inputFilesPath = "wasbs://" + inputContainerName +"@" + inputStorageAccountName + ".blob.core.windows.net/" + inputBlobPrefix

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Output files source information. This can be an environment's storage account, but not necessarily. 
# If you are using a storage account other than environment's storage account, you should manually(using AzCopy) move data to environment's storage account for loading metadata into TSI environment.
environmentStorageAccountName = ""
environmentStorageAccountAccessKey = ""
# The below container should exist and should be different that inputContainerName
environmentConatinerName = ""

# Dont edit this
outputBlobPrefix = "V=1/PT=Import/"
environmentOutputFilesPath = "wasbs://" + environmentConatinerName +"@" + environmentStorageAccountName + ".blob.core.windows.net/" + outputBlobPrefix

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# timeSeriesIdProperty should be the name of column in input parquet file which will correspond to the TSID property given while creating TSI environment.
timeSeriesIdProperty = ""

# Dont edit this
# timeStampProperty should be the name of column in input parquet file, based on which you expect to query data in TSI UI.
timeStampProperty = "timestamp"

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#DONT EDIT ANYTHING AFTER THIS

# The script will produce blobs close to this size. The blob sizes can be lesser than this sometimes due to parquet compression, but will should never be greater than OptimalFileSizeInBytes * 2
OptimalFileSizeInBytes = 20000000 # 20MB

spark.conf.set(
  "fs.azure.account.key."+inputStorageAccountName+".blob.core.windows.net",
  inputStorageAccountAccessKey)
spark.conf.set(
  "fs.azure.account.key."+environmentStorageAccountName+".blob.core.windows.net",
  environmentStorageAccountAccessKey)


#DONT change them
spark.conf.set("spark.sql.parquet.outputTimestampType","TIMESTAMP_MILLIS")
temporaryOutputPath = "tmp/"

# COMMAND ----------

# MAGIC %md <h2>Functions to get statistics on inuput blobs </h2>

# COMMAND ----------

def GetTotalDataSize(inputFilesPath):
  files = dbutils.fs.ls(inputFilesPath)
  filesDf = spark.createDataFrame(files)
  return filesDf.groupBy().sum().collect()[0][0]
  
def GetEventCountPerFile(df, inputFilesPath):
  totalEventCount = df.count()
  totalDataSize = GetTotalDataSize(inputFilesPath)
  avgEventSize = totalDataSize / totalEventCount
  return int(round(OptimalFileSizeInBytes / avgEventSize))

def getExclusiveAndInclusiveTsIdsDf(eventsDf, eventCountPerFile):
  tsIdToCountMapDf = eventsDf.groupby(timeSeriesIdProperty).count()

  exclusiveTsIdsDf = tsIdToCountMapDf.filter(tsIdToCountMapDf['count'] >= eventCountPerFile)
  inclusiveTsIdsDf = tsIdToCountMapDf.filter(tsIdToCountMapDf['count'] < eventCountPerFile).orderBy('count', ascending=False)
  
  return exclusiveTsIdsDf, inclusiveTsIdsDf

# COMMAND ----------

# MAGIC %md <h2>Functions to plan exclusive tsid partitioning</h2>

# COMMAND ----------

import bisect
from datetime import datetime

def getBounds(rdd, numPartitions, keyfunc=lambda x: x):    
  # first compute the boundary of each part via sampling: we want to partition
  # the key-space into bins such that the bins have roughly the same
  # number of (key, value) pairs falling into them
  rddSize = rdd.count()
  if not rddSize:
      return self  # empty RDD
  maxSampleSize = numPartitions * 20.0  # constant from Spark's RangePartitioner
  fraction = min(maxSampleSize / max(rddSize, 1), 1.0)
  samples = rdd.sample(False, fraction, 1).map(lambda kv: kv[0]).collect()
  samples = sorted(samples, key=keyfunc)

  # we have numPartitions many parts but one of the them has
  # an implicit boundary
  bounds = [samples[int(len(samples) * (i + 1) / numPartitions)]
            for i in range(0, numPartitions - 1)]
  return bounds

def getexclusiveTsIdsTimeStampStatsForPartitioning(eventCountPerFile, eventsDf, exclusiveTsIdsDf):
  exclusiveTsIdsTimeStampStatsForPartitioning = []
  for TsIdAndCount in exclusiveTsIdsDf.rdd.collect():  
    numPartitions = max(int(round(TsIdAndCount['count'] / eventCountPerFile)), 1)

    tsIdTimeStampsRdd = eventsDf.filter(eventsDf[timeSeriesIdProperty] == TsIdAndCount[timeSeriesIdProperty]).rdd.map(lambda x: (x[timeStampProperty], 0))

    bounds = getBounds(tsIdTimeStampsRdd, numPartitions)
    def rangePartitioner(k, keyfunc=lambda x: x):
      return bisect.bisect_left(bounds, keyfunc(k))

    timeStampPartitionedRdd = tsIdTimeStampsRdd.partitionBy(numPartitions, rangePartitioner)

    def getMinMaxPerPartition(splitIndex, iter): 
      min_value, max_value = datetime.max, datetime.min
      for value in iter:  
        if value[0] < min_value:
            min_value = value[0]
        if value[0] > max_value:
            max_value = value[0]
      return [(splitIndex, (min_value, max_value))]

    timeStampPartitionsStats = timeStampPartitionedRdd.mapPartitionsWithIndex(getMinMaxPerPartition)
    exclusiveTsIdsTimeStampStatsForPartitioning.append((TsIdAndCount[timeSeriesIdProperty], timeStampPartitionsStats.collect()))

  print("exclusiveTsIds Partitioning", exclusiveTsIdsTimeStampStatsForPartitioning)
  return exclusiveTsIdsTimeStampStatsForPartitioning

# COMMAND ----------

# MAGIC %md <h2> Functions to plan inclusive tsid partitioning </h2>

# COMMAND ----------

def getinclusiveTsIdsGroupsForPartitioning(eventCountPerFile, inclusiveTsIdsDf):
  inclusiveTsIdsGroupsForPartitioning = []
  tsIdsForCurrentPartition = []
  eventCountInCurrentPartition = 0
  for TsIdAndCount in inclusiveTsIdsDf.rdd.collect(): 
    # will reslut in file sizes of 20Mb to 40MB
    if TsIdAndCount['count'] + eventCountInCurrentPartition < (2 * eventCountPerFile):
      tsIdsForCurrentPartition.append(TsIdAndCount[timeSeriesIdProperty])
      eventCountInCurrentPartition += TsIdAndCount['count']
    else:
      inclusiveTsIdsGroupsForPartitioning.append(tsIdsForCurrentPartition)
      #print("Tsids in a group",len(tsIdsForCurrentPartition))
      tsIdsForCurrentPartition = [TsIdAndCount[timeSeriesIdProperty]]
      eventCountInCurrentPartition = TsIdAndCount['count']

  if tsIdsForCurrentPartition:
    inclusiveTsIdsGroupsForPartitioning.append(tsIdsForCurrentPartition)
    
  print("Inclsive tsid partitions", len(inclusiveTsIdsGroupsForPartitioning))
  
  return inclusiveTsIdsGroupsForPartitioning

# COMMAND ----------

# MAGIC %md <h2>Functions to get TSI partitioning plan</h2>

# COMMAND ----------

class PartitioningPlan:
  def __init__(self, isExclusive, exclsivePartioningPlan, inclusivePartitioningPlan):
    self.isExclusive = isExclusive
    self.exclsivePartioningPlan = exclsivePartioningPlan
    self.inclusivePartitioningPlan = inclusivePartitioningPlan
  
  def getPartitionId(self, timeStamp):
    if self.isExclusive :
      return self.exclsivePartioningPlan.getPartitionId(timeStamp)
    else:
      return self.inclusivePartitioningPlan.getPartitionId(timeStamp)
      
class ExclsivePartioningPlan:
  def __init__(self, rangeToPartitinIdMap):
    self.rangeToPartitinIdMap = rangeToPartitinIdMap
    
  def getPartitionId(self, timeStamp):
    for rangeToPartitinIdInfo in self.rangeToPartitinIdMap:
      if rangeToPartitinIdInfo[0].IsInRange(timeStamp):
        return rangeToPartitinIdInfo[1]

class InclusivePartioningPlan:
  def __init__(self, partitionId):
    self.partitionId = partitionId
  
  def getPartitionId(self, timeStamp):
    return self.partitionId
      
class TimeRange:
  def __init__(self, start, end):
    self.start = start
    self.end = end
  
  def IsInRange(self, timestamp):
    print("In IsRange", timestamp)
    if timestamp >= self.start and timestamp <= self.end:
      return True
    else:
      return False

def getPartitionPlan(exclusiveTsIdsTimeStampStatsForPartitioning, inclusiveTsIdsGroupsForPartitioning):
  lastPartitionId = 0
  tsIdToPartitionPlanmap = {}
  for tsIdToTimeStampStats in exclusiveTsIdsTimeStampStatsForPartitioning:
    rangeToPartitinIdMap = []
    for tsIdTotimeStampRange in tsIdToTimeStampStats[1]:
      print("timeStampRange[0]", tsIdTotimeStampRange[1][0])
      print("timeStampRange[1]", tsIdTotimeStampRange[1][1])
      timeRange = TimeRange(tsIdTotimeStampRange[1][0], tsIdTotimeStampRange[1][1])
      lastPartitionId = lastPartitionId + 1
      rangeToPartitinIdMap.append((timeRange, lastPartitionId))
      
    exclsivePartioningPlan = ExclsivePartioningPlan(rangeToPartitinIdMap)
    tsIdToPartitionPlanmap[tsIdToTimeStampStats[0]] = PartitioningPlan(True, exclsivePartioningPlan, None)


  for tsIdGroup in inclusiveTsIdsGroupsForPartitioning:
    if not tsIdGroup:
      continue
    lastPartitionId = lastPartitionId + 1
    currentPartitionId = lastPartitionId
    for tsId in tsIdGroup:
      print(tsId)
      inclusivePartioningPlan = InclusivePartioningPlan(currentPartitionId)
      tsIdToPartitionPlanmap[tsId] = PartitioningPlan(False, None, inclusivePartioningPlan)
      
  return tsIdToPartitionPlanmap, lastPartitionId

def ExecuteTsiPartitioning(inputFilesPath):
  eventsDf = sqlContext.read.parquet(inputFilesPath)
  eventCountPerFile = GetEventCountPerFile(eventsDf, inputFilesPath)
  print("EventCount per file", eventCountPerFile)
  exclusiveTsIdsDf , inclusiveTsIdsDf = getExclusiveAndInclusiveTsIdsDf(eventsDf, eventCountPerFile)
  print("Total exlusive tsids", exclusiveTsIdsDf.count())
  print("Total inclusive tsids", inclusiveTsIdsDf.count())

  tsIdToPartitionPlanmap, totalNumberOfPartitions = getPartitionPlan(getexclusiveTsIdsTimeStampStatsForPartitioning(eventCountPerFile, eventsDf, exclusiveTsIdsDf), getinclusiveTsIdsGroupsForPartitioning(eventCountPerFile, inclusiveTsIdsDf))
  print("Total number of partitions", totalNumberOfPartitions)
  
  def TsiPartitioner(k):
    return tsIdToPartitionPlanmap[k[0]].getPartitionId(k[1])
  
  keypair_rdd = eventsDf.rdd.map(lambda x : ((x[timeSeriesIdProperty],x[timeStampProperty]),x))
  partitionedkeyValueRdd = keypair_rdd.partitionBy(totalNumberOfPartitions, TsiPartitioner)
  
  partitionedRdd = partitionedkeyValueRdd.map(lambda x : x[1])

  partitionedDf = spark.createDataFrame(partitionedRdd, eventsDf.schema)

  partitionedDf.write.mode('append').parquet(environmentOutputFilesPath + temporaryOutputPath)
  
  return totalNumberOfPartitions

def RefineTsiPartitioning():
  repartitionFilePath = "repartitionTmp/"
  newPartitions = 0
  fileCount = 0
  fileList = dbutils.fs.ls(environmentOutputFilesPath + temporaryOutputPath)
  for file in fileList:
    # Repartition all the blobs that are more than 3 times the OptimalFileSizeInBytes
    if file.name.endswith(".parquet") and file.size > (OptimalFileSizeInBytes * 3):
      print("Repartitioing", file)
      dbutils.fs.mv(file.path , environmentOutputFilesPath + repartitionFilePath + file.name)
      fileCount += 1
  
  if fileCount >= 1:
    print("Files to be repartitioned", dbutils.fs.ls(environmentOutputFilesPath + repartitionFilePath))
    newPartitions = ExecuteTsiPartitioning(environmentOutputFilesPath + repartitionFilePath)
    dbutils.fs.rm(environmentOutputFilesPath + repartitionFilePath, recurse=True)
  return newPartitions
      

# COMMAND ----------

# MAGIC %md <h2> Perform TSI Partitioning </h2>

# COMMAND ----------

ExecuteTsiPartitioning(inputFilesPath)

# COMMAND ----------

# MAGIC %md <h2> Refine the output blobs </h2>

# COMMAND ----------

while True:
  newPartitions = RefineTsiPartitioning()
  if newPartitions == 0:
    break

# COMMAND ----------

# MAGIC %md <h2> Rename the output blobs as required by TSI </h2>

# COMMAND ----------

from datetime import datetime, timedelta

eventSource = "historic"
partitionId = "33"

fileList = dbutils.fs.ls(environmentOutputFilesPath + temporaryOutputPath)

outputBlobUris = []
date = datetime.now()
for i in range(len(fileList)): 
    date += timedelta(seconds=1)
    uri = environmentOutputFilesPath + "Y=" + str(date.year) + "/M=" + str(date.month).rjust(2,'0') + "/" + date.strftime('%Y%m%d%H%M%S%f')[:17] + "_" + eventSource + "_" + partitionId + ".parquet"
    outputBlobUris.append(uri)
    
print(outputBlobUris)

index = 0
for file in fileList:
  if file.name.endswith(".parquet") and file.size>0:
    print(file)
    dbutils.fs.mv(file.path, outputBlobUris[index])
    index += 1
    
dbutils.fs.rm(environmentOutputFilesPath + temporaryOutputPath , recurse=True)

# COMMAND ----------

# MAGIC %md <h2> Validating data</h2>

# COMMAND ----------

inputEventsDf = sqlContext.read.parquet(inputFilesPath)
inputEventCount = inputEventsDf.count()
print(inputEventCount)

outputEventsDf = sqlContext.read.parquet(environmentOutputFilesPath)
outputEventsCount = outputEventsDf.count()

print(outputEventsCount)

if inputEventCount != outputEventsCount:
  print("DATA CORRUPTED !!")
else:
  print("TSI partitioning completed succesfully!")
