## BIG QUERY SETUP
### External Table Creation
```bash
CREATE OR REPLACE EXTERNAL TABLE `round-fold-449120-g9.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2025_xcluo/yellow_tripdata_2024-*.parquet']
);
```

### Materialized Table Creation
#### using BigQuery UI:

- Choose dataset name and click 'Create Table'

- Choose Create Table from 'Google Cloud Storage'

- Choose File Name '<bucket_name>/yellow_tripdata_2024-*.parquet'

- Choose File Format 'parquet'

- Choose Destination: Project Name, Dataset Name, Table Name

- Choose 'Native Table' type

- Click 'No Partitioning'

- Click 'Create Table' and finish


## Question 1
```bash
SELECT COUNT(*) AS total_counts
FROM `round-fold-449120-g9.nytaxi.external_yellow_tripdata`;
```
#### Answer: 20332093

## Question 2
###  Expected Behavior for Data Scanned in BigQuery

| **Table Type**          | **How It Works** | **Expected Data Scanned** |
|-------------------------|----------------|----------------------|
| **External Table (GCS)** | Reads directly from **Google Cloud Storage (GCS)**. Since Parquet is **columnar**, it only reads the required column (`PULocationID`). | **18.82 MB** |
| **Materialized Table ** | Fully loaded inside BigQuery. Queries scan the `PULocationID` column in all rows. Since **no partitioning or clustering**, more data is scanned. | **47.60 MB** |


## Question 3
```bash
SELECT PULocationID
FROM `round-fold-449120-g9.nytaxi.materialized_yellow_taxi_data`;
```
```bash
SELECT PULocationID, DOLocationID
FROM `round-fold-449120-g9.nytaxi.materialized_yellow_taxi_data`;
```

## Question 4
```bash
SELECT COUNT(*)
FROM `round-fold-449120-g9.nytaxi.materialized_yellow_taxi_data`
WHERE fare_amount = 0;
```
#### Answer: 8333


## Question 5
Divides the table into segments based on a column (e.g., tpep_dropoff_datetime). Queries that filter by this column only scan relevant partitions, reducing scanned data.

Organizes the data based on column values (e.g., VendorID). Queries that order or filter by this column perform better as similar values are stored together.

#### SQL to create optimized table
```bash
CREATE OR REPLACE TABLE `round-fold-449120-g9.nytaxi.optimized_yellow_taxi_data`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `round-fold-449120-g9.nytaxi.materialized_yellow_taxi_data`;
```
#### Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID


## Question 6
#### For Materalized Table
```bash
SELECT DISTINCT VendorID
FROM `round-fold-449120-g9.nytaxi.materialized_yellow_taxi_data`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

#### For Partitioned Table
```bash
SELECT DISTINCT VendorID
FROM `round-fold-449120-g9.nytaxi.optimized_yellow_taxi_data`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```
#### Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

## Question 7
When creating an External Table in BigQuery, the data is not stored in BigQuery itself but remains in its original location - **GCP**.
```bash
SELECT * FROM `round-fold-449120-g9.nytaxi.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE table_name = 'external_yellow_tripdata';
```
in “uris” field, we can see the data is stored in Google Cloud Storage (GCP Bucket).

## Question 8
there are situations where Clustering is NOT Recommended:
- If the queries don’t filter/order by the clustered column
- For small tables (under 1GB): because BigQuery already optimizes small tables, clustering won’t help much.
- If the column has low cardinality
#### Answer: False

## Question 9
```bash
SELECT COUNT(*)
FROM `round-fold-449120-g9.nytaxi.materialized_yellow_taxi_data`;
```
I got result **0** in  “Bytes Processed”

BigQuery does not need to scan the full dataset because when creating the materialized table, the row count is precomputed in the metadata table.
