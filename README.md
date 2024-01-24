# pv-pipeline

# What is it?
An ETL pipeline to extract and prepare photovoltaic output data (aka PV data) for ML model training and data analysis. The data is from the [Open Energy Data Initiative](https://data.openei.org/submissions/4568).

# Why does it exist?
I am working with [Open Climate Fix](https://openclimatefix.org/) on models that forecast the amount of energy a solar panel site will generate in the near future (aka PV nowcasting). One of the model inputs is historical PV data, the amount of energy a given solar panel site has generated in the past. The reasons for building this pipeline are:

- To expand our dataset. This pipeline extracts historical PV data from an S3 bucket, processes the data and stores it in a GCP storage bucket to be analyzed in BigQuery. This data will be used to train models soon.

- To show the data acquisition team at Cohere that I have basic data pipelining skills.

# What tools are used?
- **AWS S3** as the data source
- **Airflow** for task configuration and scheduling*
- **Spark** as the data processing engine*
- **BigQuery** as the data destination
- **Docker** to run things smoothly on my Windows PC

(*) see comments section below for justification about using Spark and Airflow.

# Data Schema
The data has a **star schema**. This is my first time designing my own data schema, so expect incompentency. The data is extracted from the source, processed, joined, and loaded into BigQuery. The resulting database has the following schema.

### Facts Table
In the center is a facts table containing time series data. Each row contains:
- `timestamp`: when the data was collected
- `ss_id`: solar system ID
- `metric_id`: ID of metric measured
- `value`: value of metric

timestamp, system_id and metric_id form a primary key.

### Dim1: System Metadata
The first dimension table contains metadata about each site. Some but not all columns:
- `ss_id`: solar system ID (primary key)
- `latitude`: decimal latitude geo location
- `longitude`: decimal longitude geo location
- `elevation`: distance in meters above sea level, nullable
- `av_pressure`: average annual atmospheric pressure at site in psi
- `av_temp`: average ambient temperature in degrees Celsius at site
- `climate_type`: The Koppen-Geiger classifier for the site location
- `mount_azimuth`: azimuth angle of mount point in degrees
- `mount_tilt`: tilt angle of mount pointing in degrees
None of these data change.

### Dim2: Metrics Metadata
The second dimension table contains metadata about metrics. Each system uniquely gathers and identifies metrics (DC power, solar irradiance, module temperature, etc), and hence the metrics metadata is needed to identify metrics for each system. The columns of this table are:
- `ss_id`: associated solar system ID
- `metric_id`: primary key of the metric
- `common_name`: a general grouping of sensor types (e.g. DC voltage, AC energy, POA irradiance)
- `raw_units`: raw unscaled or uncalibrated units of the values produced by the sensor

None of these data change.

`ss_id` and `metrics_id` form a primary key.

# Comments
### Was Airflow a good choice?
Not really. Since this pipeline does not run on a schedule and does not have complex dependencies between tasks, a simple cron job or even manual execution would suffice. However, I now have infrastructure to easily add more tasks with dependencies. I can also easily monitor task statuses through the Airflow Web UI.

### Was Spark a good choice?
I do not know. This was my first time using Spark and I am still understanding its use case. The power of Spark is in parallel processing across multiple nodes. Without access to a multi-node machine or the bugdet to run multiple machines in the cloud I am not truly leveraging Spark. Running Dask on my PC would have been simpler and cheaper. But I am using PySpark as a learning exercise.

This pipeline will process over a terabyte of PV data. I imagine it can do that over a couple of days on my PC using Dask. However, if we had access to a multi-node computer and expected to process tons of data quickly, then having a Spark environment set up is beneficial.

### Was BigQuery a good choice?
Again, not sure. I do not have the budget to store and analyze all the data in BigQuery. I don't have a better alternative either. I will just have to analyze this data locally using SparkSQL or Dask. I used BigQuery as a learning exercise.

### I learned how to:
- extract data from an S3 bucket using boto3
- load data into BigQuery
- configure a BigQuery table partition
- design a database schema
- set up an Airflow environment
- set up a Spark session
- write PySpark code

# Next steps
- Write a docker compose file to build this pipeline.
- Migrate the pipeline to run in the cloud, or on WAT.ai's supercomputer, Nebula.
- Allow Spark to work across multiple nodes.
- Make the pipeline robust to nulls, corrupt data and unexpected types. (we need this badly lol)
- Better logging during task execution.



### Sources:
I copied plenty of code and text form the following sources:
- [Setting up Airflow with Docker on Windows.](https://medium.com/@garc1a0scar/how-to-start-with-apache-airflow-in-docker-windows-902674ad1bbe)
- [openEDI data documentation](https://github.com/openEDI/documentation/blob/main/pvdaq.md)