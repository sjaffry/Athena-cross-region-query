CREATE TABLE "nyctaxi-data-db-oregon"."oregon_yellow_aggregated"
                WITH (
                    format = 'Parquet',
                    parquet_compression = 'SNAPPY',
                    partitioned_by=array['day'], 
                    external_location = 's3://sj-nyctaxi-data-oregon/yellow_aggregated/')
                AS 
SELECT vendor_name, sum(trip_distance) as "total_distance" ,
substr("pickup_datetime",9,2) AS "day"
FROM "nyctaxi-data-db-oregon"."yellow" 
GROUP BY vendor_name, substr("pickup_datetime",9,2) LIMIT 100;