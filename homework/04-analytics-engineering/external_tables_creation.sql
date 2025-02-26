-- green trip data
CREATE OR REPLACE EXTERNAL TABLE `round-fold-449120-g9.trips_data_all.green_tripdata`
OPTIONS (
  format = "PARQUET",
  uris = ["gs://dtc-data-lake-xcluo/green/*.parquet"]
);

-- yellow trip data
CREATE OR REPLACE EXTERNAL TABLE `round-fold-449120-g9.trips_data_all.yellow_tripdata`
OPTIONS (
  format = "PARQUET",
  uris = ["gs://dtc-data-lake-xcluo/yellow/*.parquet"]
);

-- fhv trip data
CREATE OR REPLACE EXTERNAL TABLE `round-fold-449120-g9.trips_data_all.fhv_tripdata`
OPTIONS (
  format = "PARQUET",
  uris = ["gs://dtc-data-lake-xcluo/fhv/*.parquet"]
);

SELECT COUNT(*) AS row_count 
FROM `round-fold-449120-g9.trips_data_all.fhv_tripdata`;

