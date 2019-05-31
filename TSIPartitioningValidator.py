# Databricks notebook source
storage_account_name = ""
storage_account_access_key = ""
outputContainerName = ""
timeSeriesIdProperty = ""

#===================================================================================================================
# Do not edit anything below
#---------------------------------------------------------------------------------------------------------------
outputFilesPath = "wasbs://"+ outputContainerName +"@" + storage_account_name + ".blob.core.windows.net/" 
outputFilesPathPrefix = "V=1/PT=Import/"

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


timeStampProperty = "timestamp"
    

# COMMAND ----------

class InclusiveTsIdValidator:
  def __init__(self):
    self.tsids = []
    
  def validateAndStore(self, newTsIds):
    for tsid in newTsIds:
      if tsid in self.tsids:
        print("Violation!! Invlusive TsID repeated in another inclusive blob", tsid)
        return False
      else:
        self.tsids.append(tsid)
        return True
      
  def validateExclsiveIds(self, exclusiveTsIds):
    for tsid in exclusiveTsIds:
      if tsid in self.tsids:
        print("Violation!! exclusive TsID is repeated in a inclusive blob", tsid)
        return False 
    return True
        
class ExclusiveTsIdValidator:
  def __init__(self):
    self.tsidStats = {}
    
  def validateAndStore(self, tsid, time_range):
    if tsid in self.tsidStats:
      for existing_time_range in self.tsidStats[tsid]:
        if existing_time_range.is_overlapped(time_range):
          print("Violation!! Exclusive TsID time stamp overlap", tsid, existing_time_range, time_range)
          return False
        else:
          self.tsidStats[tsid].append(time_range)
          return True
    else:
      self.tsidStats[tsid] = [time_range]
      return True
      
  def getTsIds(self):
    return self.tsidStats.keys()
    
class Range:
  def __init__(self, start, end):
    self.start = start
    self.end = end
    
  def is_overlapped(self, time_range):
    if max(self.start, time_range.start) < min(self.end, time_range.end):
        return True
    else:
        return False
    

# COMMAND ----------

from azure.storage.blob import BlockBlobService
import math

def convert_size(size_bytes):
  if size_bytes == 0:
    return "0B"
  size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
  i = int(math.floor(math.log(size_bytes, 1024)))
  p = math.pow(1024, i)
  s = round(size_bytes / p, 2)
  return "%s %s" % (s, size_name[i])

block_blob_service = BlockBlobService(account_name=storage_account_name, account_key=storage_account_access_key)

generator = block_blob_service.list_blobs(outputContainerName, prefix=outputFilesPathPrefix)

# COMMAND ----------

inclusiveIdValidator = InclusiveTsIdValidator()
exclusiveTsIdValidator = ExclusiveTsIdValidator()

for blob in generator:
  if blob.name.endswith(".parquet") and blob.properties.content_length>0:
    eventsDf = sqlContext.read.parquet(outputFilesPath + blob.name)
    tsids = eventsDf.select(timeSeriesIdProperty).distinct().collect()
    eventCount = eventsDf.count()
    
    # Inclusive tsids should be present in only one blob 
    if(len(tsids) > 1):
      if not inclusiveIdValidator.validateAndStore(tsids):
        print("Blob has repeated Ids :" +blob.name, "tsiDs =" ,len(tsids), "eventCount=" , eventCount, "blobSize=", convert_size(blob.properties.content_length))
    
    # Exclsuive tsids if divided into multiple blobs, should be time sorted.
    if(len(tsids) == 1):
      print("Its here")
      if not exclusiveTsIdValidator.validateAndStore(tsids[0], Range(eventsDf.agg({timeStampProperty: "min"}).collect()[0][0], eventsDf.agg({timeStampProperty: "max"}).collect()[0][0])):
        print("Blob is not partitioned by time :" +blob.name, "tsiDs =" ,tsids, "eventCount=" , eventCount, "blobSize=", convert_size(blob.properties.content_length))   
        
    # checks if any of the exclusive TsIds are part of inclusive blobs
    inclusiveIdValidator.validateExclsiveIds(exclusiveTsIdValidator.getTsIds())
    
    

# COMMAND ----------

from pyspark.sql import Row

blobSizes = []
blobsBetween20Mband60Mb = 0
for blob in generator:
  if blob.name.endswith(".parquet") and blob.properties.content_length>0:
    if blob.properties.content_length > 80000000:
      print("Violation: Blob size exceeds 80MB")
    if blob.properties.content_length > 20000000 and blob.properties.content_length < 60000000:
      blobsBetween20Mband60Mb += 1
    blobSizes.append(blob.properties.content_length)
  
# 80% is not hard limit. Its ok if its close to 80%
if blobsBetween20Mband60Mb/ len(blobSizes) < 0.8:
  print("Violation!! : Atleast 80% of the blobs should be in the range of 20MB to 60MB")
  
rdd1 = sc.parallelize(blobSizes)
row_rdd = rdd1.map(lambda x: Row(x))
filesDf = sqlContext.createDataFrame(row_rdd,['blobSizes'])
filesDf.describe().show()
#display(filesDf)

