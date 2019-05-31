# Instructions

The purpose of this repository is to test Seeding a Time Series Instance Database with Historical Data


## Extract the desired historical data

This sample conversion is using data from the OSIsoft Example Kit: [Pump Condition Based Maintenance](https://pisquare.osisoft.com/community/all-things-pi/af-library/asset-based-pi-example-kits)

Data is extracted from the PI System using the `extract.ps1` which results in the `sample.csv`

__Rules__

- Data must be flattened
- Each row must at a minimum have
    - The partition key value used as the Time Series ID
    - The timestamp representing the time of the event `yyy-MM-ddThh:mm:ss.fffZ`
    - The event measurements

__Steps__

1. RDP to the Virtual Training Environment
2. Copy and execute the powershell script `extract.ps1`
3. Copy the csv file results `sample.csv`


## Convert the historical data to a Parquet file format

There are many different ways that this data can now be converted from CSV to Parquet file formats.  This method is using a Databricks Notebook.

__Rules__

- The Time Series Id column name must be the same as defined when creating the TSI environment
    - The column name must have `_string` appended, e.g. “DeviceID_string” and must be of type StringType
    - Only single key property is supported
- The Event Time column must be named `timestamp` and be of type TimestampeType
- All other column names must be appended with the time
    - Supported Types:  _string (UTF8), _bool, _datetime, _double
- Compression should be Snappy

__Steps__

1. Load the CSV file into Blob Storage `convert\sample.csv`
2. Modify and import the `CsvToParquet.scala` notebook with the proper storage account information.

```scala
val storageAccountName = ""
val storageAccountAccessKey = ""
```

3. Download the converted file which is in the parquet folder.  `part-xxxx...snappy.parquet`

4. Load the Parquet file into Blob Storage `convert\sample.parquet`

5. Execute and Run the `CsvToParquet` Notebook


## Partion the historical data Parquet file

The historical data has to be partioned in a manner that is now compatable with TSI.

__Rules__

- Databricks cluster should not use GPU Types

__Steps__

1. Modify the `TSIPartioning.py` notebook with the proper input storage account information

```python
# Input files source information
inputStorageAccountName = ""
inputStorageAccountAccessKey = ""
inputContainerName = "conversion"

# blob path inside container
inputBlobPrefix = "convert/sample.parquet"
```

2. Modify the `TSIPartioning.py` notebook with the proper TSI environment storage account information

```python
# Output files source information. This can be an environment's storage account, but not necessarily. 
# If you are using a storage account other than environment's storage account, you should manually(using AzCopy) move data to environment's storage account for loading metadata into TSI environment.
environmentStorageAccountName = ""
environmentStorageAccountAccessKey = ""
# The below container should exist and should be different that inputContainerName
environmentContainerName = "partition"
```

3. Modify the `TSIPartioning.py` notebook with the proper TSI Time Series Id Property

```python
# timeSeriesIdProperty should be the name of column in input parquet file which will correspond to the TSID property given while creating TSI environment.
timeSeriesIdProperty = "deviceId_string"
```

4. Import and Execute the `TSIPartioning` Notebook


## Validate the TSI Partioned Historical Data

The partioned data can now be validated as compatable with Time Series Instances.

__Rules__

- Databricks cluster should not use GPU Types
- Databricks cluster must have the following library installed _Name: `Azure`  Type: PyPI_

__Steps__

1. Modify the `TSIPartioningValidator.py` notebook with the proper  storage account information


```python
# Databricks notebook source
storage_account_name = ""
storage_account_access_key = ""
outputContainerName = "partition"
timeSeriesIdProperty = "deviceId_string"
```

4. Import and Execute the `TSIPartioningValidator` Notebook


## Send the required information to Microsoft Engineering

```python
# Databricks notebook source
storage_account_name = ""
storage_account_access_key = ""
containerName = "partition"
tsiEnvContainerName = "env-111a1111-11d1-1111-1111-111111111111"
```