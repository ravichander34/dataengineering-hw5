"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: Provider that provided the record
  - name: pickup_datetime
    type: timestamp
    description: When the trip started
  - name: dropoff_datetime
    type: timestamp
    description: When the trip ended
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: float
    description: Trip distance in miles
  - name: pickup_location_id
    type: integer
    description: Pickup location ID
  - name: dropoff_location_id
    type: integer
    description: Dropoff location ID
  - name: RatecodeID
    type: integer
    description: Rate code for the trip
  - name: store_and_fwd_flag
    type: string
    description: Whether trip was stored and forwarded
  - name: payment_type
    type: integer
    description: Payment type ID
  - name: fare_amount
    type: float
    description: Base fare amount
    primary_key: true
  - name: extra
    type: float
    description: Extra charges
  - name: mta_tax
    type: float
    description: MTA tax
  - name: tip_amount
    type: float
    description: Tip amount
  - name: tolls_amount
    type: float
    description: Tolls amount
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge
  - name: total_amount
    type: float
    description: Total amount charged
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge
  - name: Airport_fee
    type: float
    description: Airport fee
  - name: taxi_type
    type: string
    description: Type of taxi (yellow/green)
  - name: extracted_at
    type: timestamp
    description: When this data was extracted

@bruin"""

import os
import json
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Uses BRUIN_START_DATE/BRUIN_END_DATE and taxi_types variable to determine
    which parquet files to download and ingest.
    """
    # Get date range from Bruin environment
    start_date = datetime.strptime(os.environ['BRUIN_START_DATE'], '%Y-%m-%d')
    end_date = datetime.strptime(os.environ['BRUIN_END_DATE'], '%Y-%m-%d')
    
    # Get taxi types from pipeline variables
    bruin_vars = json.loads(os.environ.get('BRUIN_VARS', '{}'))
    taxi_types = bruin_vars.get('taxi_types', ['yellow', 'green'])
    
    # Base URL for NYC TLC data
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    
    # Generate list of files to fetch
    dataframes = []
    current_date = start_date
    
    while current_date <= end_date:
        year_month = current_date.strftime('%Y-%m')
        
        for taxi_type in taxi_types:
            file_url = f"{base_url}/{taxi_type}_tripdata_{year_month}.parquet"
            
            try:
                print(f"Fetching {file_url}")
                df = pd.read_parquet(file_url)
                
                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.now()
                
                # Standardize column names
                df.columns = df.columns.str.lower()
                if 'tpep_pickup_datetime' in df.columns:
                    df = df.rename(columns={
                        'tpep_pickup_datetime': 'pickup_datetime',
                        'tpep_dropoff_datetime': 'dropoff_datetime',
                        'pulocationid': 'pickup_location_id',
                        'dolocationid': 'dropoff_location_id'
                    })
                elif 'lpep_pickup_datetime' in df.columns:
                    df = df.rename(columns={
                        'lpep_pickup_datetime': 'pickup_datetime',
                        'lpep_dropoff_datetime': 'dropoff_datetime',
                        'pulocationid': 'pickup_location_id',
                        'dolocationid': 'dropoff_location_id'
                    })
                
                dataframes.append(df)
                print(f"Successfully fetched {len(df)} rows")
                
            except Exception as e:
                print(f"Failed to fetch {file_url}: {e}")
        
        # Move to next month
        current_date += relativedelta(months=1)
    
    # Combine all dataframes
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        print(f"Total rows ingested: {len(final_df)}")
        return final_df
    else:
        print("No data fetched")
        return pd.DataFrame()

