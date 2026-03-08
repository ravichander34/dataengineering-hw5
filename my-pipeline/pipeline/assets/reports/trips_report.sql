/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: Date of pickup
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow/green)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment type name
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: integer
    description: Number of trips
    checks:
      - name: non_negative
      - name: positive
  - name: total_fare_amount
    type: float
    description: Total fare amount (can be negative due to refunds)
  - name: total_distance
    type: float
    description: Total distance traveled
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: float
    description: Average fare amount per trip (can be negative due to refunds)
  - name: avg_distance
    type: float
    description: Average distance per trip
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  CAST(pickup_datetime AS DATE) AS pickup_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare_amount,
  SUM(trip_distance) AS total_distance,
  AVG(fare_amount) AS avg_fare_amount,
  AVG(trip_distance) AS avg_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type_name
