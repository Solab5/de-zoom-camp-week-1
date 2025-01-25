# de-zoom-camp-week-1

## Question 1. Understanding docker first run
- docker run -it python:3.12.8 bash
- pip --version gives `24.3.1`

## Question 2. Understanding Docker networking and docker-compose
- hostname is `db`
- port is `5432`

### running postgres:
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13

### ingesting data via scrip
- trips
`URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz`
```
python upload_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_data \
  --url=${URL}
```

- zones
`URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv`

```
python upload_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=taxi_zone \
  --url=${URL}
```

## Question 3: Trip Segmentation Count
`docker-compose up` - to run pgadmin
1. 
```
SELECT 
	COUNT(*) trips
FROM 
	green_taxi_data
WHERE
	lpep_pickup_datetime >= '2019-10-01'
AND
	lpep_pickup_datetime < '2019-11-01' 	
AND 
	trip_distance <= 1
```
`104830`
2. 
```
SELECT 
	count(*) as trips
FROM 
	green_taxi_data
WHERE
	trip_distance > 1
AND
	trip_distance <= 3
AND
	lpep_pickup_datetime >= '2019-10-01'
AND
	lpep_pickup_datetime < '2019-11-01'
```
`198995`

3. 
```
SELECT 
	count(*) as trips
FROM 
	green_taxi_data
WHERE
	trip_distance > 3
AND
	trip_distance <= 7
AND
	lpep_pickup_datetime >= '2019-10-01'
AND
	lpep_pickup_datetime < '2019-11-01'
```
`109642`

4. 
```
SELECT 
	count(*) as trips
FROM 
	green_taxi_data
WHERE
	trip_distance > 7
AND
	trip_distance <= 10
AND
	lpep_pickup_datetime >= '2019-10-01'
AND
	lpep_pickup_datetime < '2019-11-01'
```
`27686`

5. 
```
SELECT 
	count(*) as trips
FROM 
	green_taxi_data
WHERE
	trip_distance >= 10
AND
	lpep_pickup_datetime >= '2019-10-01'
AND
	lpep_pickup_datetime < '2019-11-01'
```
`35293`

## Question 4
```
SELECT 
	lpep_pickup_datetime::date,
	MAX(trip_distance) as max_dist
FROM 
	green_taxi_data
GROUP BY
	lpep_pickup_datetime::date
ORDER BY
	max_dist DESC
LIMIT 1
```
`2019-10-31 -- 515.89`

## Question 5. Three biggest pickup zones
```
SELECT 
	gt."PULocationID",
	tz."Zone",
	SUM(gt.total_amount) as total_revenue	
FROM 
	green_taxi_data gt
JOIN
	taxi_zone tz ON gt."PULocationID" = tz."LocationID"	
WHERE
	lpep_pickup_datetime::date = '2019-10-18'
GROUP BY
	"PULocationID", "Zone"
HAVING
	SUM(total_amount) > 13000
ORDER BY
	total_revenue DESC
```
`East Harlem North, East Harlem South, Morningside Heights`

## Question 6. Largest tip
```
SELECT 
	tz."Zone",
	MAX(gt.tip_amount)
FROM 
	green_taxi_data gt
JOIN
	taxi_zone tz ON gt."PULocationID" = tz."LocationID"	
WHERE
	DATE_TRUNC('month', gt.lpep_pickup_datetime) = '2019-10-01'
AND
	tz."Zone" = 'East Harlem North'
GROUP BY
	tz."Zone"

```
`East Harlem North`