
CREATE DATABASE TestDB
CREATE EXTERNAL DATA SOURCE YellowTaxi
WITH ( LOCATION = 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/')


CREATE EXTERNAL DATA SOURCE destination_ds
WITH
(    
	LOCATION         = 'https://mtbanklake1234.dfs.core.windows.net/taxidata/'
)

CREATE EXTERNAL FILE FORMAT parquet_file_format
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


CREATE EXTERNAL TABLE ProcessedYellowTaxis
WITH 
(  
    DATA_SOURCE = destination_ds,
    FILE_FORMAT = parquet_file_format,
    LOCATION='/Facts/'
) 
AS

SELECT TOP 100 
      PickupTime        = tpepPickupDateTime
    , DropTime          = tpepDropoffDateTime
    , Passengers        = passengercount
    , TripDistance      = tripDistance
    , RateCodeId        = RatecodeID
    , PickupLocationId  = PULocationID
    , DropLocationId    = DOLocationID
    , PaymentTypeId     = paymenttype
    , TotalAmount       = totalAmount  
,TripYear =year(tpepPickupDateTime)
,TripMonth=month(tpepPickupDateTime)
,TripDay =Day(tpepPickupDateTime) 
FROM
OPENROWSET( 
        BULK 'puYear=2019/puMonth=*/*.snappy.parquet', 
        DATA_SOURCE = 'YellowTaxi', 
        FORMAT='PARQUET' 
    ) as result


