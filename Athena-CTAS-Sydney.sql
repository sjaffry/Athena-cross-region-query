CREATE TABLE "nyctaxi-data-db-sydney"."sydney_yellow_aggregated"
                WITH (
                    format = 'Parquet',
                    parquet_compression = 'SNAPPY',
                    partitioned_by=array['day'], 
                    external_location = 's3://sj-nyctaxi-data-sydney/yellow_aggregated/')
                AS 
SELECT vendor_name, sum(trip_distance) as "total_distance" ,
substr("pickup_datetime",9,2) AS "day"
FROM "nyctaxi-data-db-sydney"."yellow" 
GROUP BY vendor_name, substr("pickup_datetime",9,2) LIMIT 100;