-- green trip data
CREATE OR REPLACE EXTERNAL TABLE `round-fold-449120-g9.trips_data_all.green_tripdata`
OPTIONS (
  format = "CSV",  
  uris = ["gs://dtc-data-lake-xcluo/green/*.csv.gz"],
  skip_leading_rows = 1
);

-- yellow trip data
CREATE OR REPLACE EXTERNAL TABLE `round-fold-449120-g9.trips_data_all.yellow_tripdata`
OPTIONS (
  format = "CSV",  
  uris = ["gs://dtc-data-lake-xcluo/yellow/*.csv.gz"],
  skip_leading_rows = 1 
);

-- fhv trip data
CREATE OR REPLACE EXTERNAL TABLE `round-fold-449120-g9.trips_data_all.fhv_tripdata`
OPTIONS (
  format = "CSV",  
  uris = ["gs://dtc-data-lake-xcluo/fhv/*.csv.gz"],
  skip_leading_rows = 1 
);





