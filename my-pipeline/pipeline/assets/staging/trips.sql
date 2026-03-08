/* @bruin

name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the trip started
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: When the trip ended
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: Pickup location ID
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: Dropoff location ID
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Base fare amount
    primary_key: true
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: Number of passengers
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: Human-readable payment type
    checks:
      - name: not_null
  - name: total_amount
    type: float
    description: Total amount charged
  - name: taxi_type
    type: string
    description: Type of taxi (yellow/green)
    checks:
      - name: not_null

@bruin */

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
      ORDER BY extracted_at DESC
    ) AS row_num
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND fare_amount IS NOT NULL
)

SELECT
  t.vendorid,
  t.pickup_datetime,
  t.dropoff_datetime,
  t.passenger_count,
  t.trip_distance,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.ratecodeid,
  t.store_and_fwd_flag,
  t.payment_type,
  COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.total_amount,
  t.congestion_surcharge,
  t.airport_fee,
  t.taxi_type,
  t.extracted_at
FROM deduplicated t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type = p.payment_type_id
WHERE t.row_num = 1
