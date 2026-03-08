
bruin - unified cli tool 

1 .In a Bruin project, what are the required files/directories?

a bruin.yml and assets/
b .bruin.yml and pipeline.yml (assets can be anywhere)
c .bruin.yml and pipeline/ with pipeline.yml and assets/
d pipeline.yml and assets/ only

answer is c - .bruin.yml and pipeline/ with pipeline.yml and assets/ 


2. 
You're building a pipeline that processes NYC taxi data organized by month based on pickup_datetime. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

append - always add new rows
replace - truncate and rebuild entirely
time_interval - incremental based on a time column
view - create a virtual table only

answer is c  - time_interval - incremental based on a time column 

3. 
You have the following variable defined in pipeline.yml:

variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
How do you override this when running the pipeline to only process yellow taxis?

bruin run --taxi-types yellow
bruin run --var taxi_types=yellow
bruin run --var 'taxi_types=["yellow"]'
bruin run --set taxi_types=["yellow"]

ans is c - taxi_types in quotes bruin run --var 'taxi_types=["yellow"]'

4. You've modified the ingestion/trips.py asset and want to run it plus all downstream assets. Which command should you use?

bruin run ingestion.trips --all
bruin run ingestion/trips.py --downstream
bruin run pipeline/trips.py --recursive
bruin run --select ingestion.trips+

ans is d - bruin run --select ingestion.trips+

5. You want to ensure the pickup_datetime column in your trips table never has NULL values. Which quality check should you add to your asset definition?

name: unique
name: not_null
name: positive
name: accepted_values, value: [not_null]

ans is b - name: not_null

6. After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

bruin graph
bruin dependencies
bruin lineage
bruin show

ans is c - bruin lineage 

7. You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

--create
--init
--full-refresh
--truncate

ans is c - full-refresh 