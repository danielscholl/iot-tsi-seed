# Instructions

This sample conversion is using data from the OSIsoft Example Kit: [Pump Condition Based Maintenance](https://pisquare.osisoft.com/community/all-things-pi/af-library/asset-based-pi-example-kits)

Data is extracted from the PI System using the `script.ps1` which results in the `sample.csv`

## Convert the data to Parquey file format

1. Load the sample.csv to Blob Storage
2. Modify the `CsvToParquet.scala` notebook to set the account storage information

```scala
val storageAccountName = ""
val storageAccountAccessKey = ""
```

3. Download the converted file which is in the parquet folder.  `part-xxxx...snappy.parquet`

4. Upload to the