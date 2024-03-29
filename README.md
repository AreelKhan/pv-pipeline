# Cohere ETL pv-pipeline mini project

# What is it?
An ETL pipeline to extract and prepare photovoltaic output data (aka PV data) for ML model training and data analysis. The data is from the [Open Energy Data Initiative](https://data.openei.org/submissions/4568).

# Why build this pipeline?

- To show the data acquisition team at Cohere that I have basic data pipelining skills.
- To expand a dataset we use at a research lab to train models that forecast the amount of energy a solar panel will generate.

# What tools are used?
- **AWS S3** as the data source
- **Airflow** for task configuration and scheduling*
- **Spark** and **Dask** as the data processing engine*
- **BigQuery** as the data destination
- **Docker** to run things smoothly on my Windows PC

(*) see comments section below for justification about using Spark and Airflow.

# Data Schema
The data has a **star schema**. This is my first time designing my own data schema, so expect incompentency.

The data is extracted from the source, processed, joined, and loaded into BigQuery. The resulting database has the following schema.

### Facts Table
In the center is a facts table containing time series data. Each row contains:
| Name        | Description |
| ----------- | ----------- |
| `ss_id`     | Solar system ID |
| `timestamp` | When value is measure |
| `sensor`    | sensor name |
| `units`     | unit of measurement |
| `value`     | measured value in units |

`timestamp`, `system_id` and `sensor` form a primary key.

### Dim1: System Metadata
The first dimension table contains metadata about each site. Some but not all columns:
| Name        | Description |
| ----------- | ----------- |
|`ss_id`| solar system ID (primary key)|
|`latitude`| decimal latitude geo location|
|`longitude`| decimal longitude geo location|
|`elevation`| distance in meters above sea level, nullable|
|`av_pressure`| average annual atmospheric pressure at site in psi|
|`av_temp`| average ambient temperature in degrees Celsius at site|
|`climate_type`| The Koppen-Geiger classifier for the site location|
|`mount_azimuth`| azimuth angle of mount point in degrees|
|`mount_tilt`| tilt angle of mount pointing in degrees|

`ss_id` forms a unique key. This data is static.

# How to run pipeline.
There are two pipelines:
- `pv-metadata-pipeline`: generates the metadata dimension table.
- `pv-spark-pipeline` and `pv-dask-pipeline`: both generate the facts tables, except one uses Spark while the other uses Dask.

At the moment these pipelines are unscheduled and can be manually executed from the airflow UI or CLI.

You must provide a DAG config (a JSON object) to the pipeline at run time. It will contain information like the start/end dates as well as the `ss_id` of the solar panel system being processed 

# Comments
### Was Airflow a good choice?
Not really. Since this pipeline does not run on a schedule and does not have complex dependencies between tasks, manual execution would suffice. But it was fun using Airflow.

### Was Spark a good choice?
I do not know. This was my first time using Spark so I am still understanding its use case. The power of Spark is in parallel processing across multiple nodes, that's not something I did. I feel running Dask was a lot simpler. I used PySpark as a learning exercise.

### Was BigQuery a good choice?
I think so. Since I need a database for analytics, and the data is around 500 GB, BQ seemed approriate. Ideally I would have the budget to store and analyze the full dataset. But I don't. I will have to analyze this data locally using SparkSQL or Dask. I used BigQuery as a learning exercise.

### I learned how to:
- extract data from an S3 bucket using boto3
- load data into BigQuery table using Python API
- configure BigQuery service account
- configure a BigQuery table partition
- design a database schema
- set up an Airflow environment
- debug Airflow DAGs in more depth
- set up a Spark session
- integrate Spark with Airflow
- write PySpark code
- write my own docker-compose file
- some new Linux commands

# Next steps
- Migrate the pipeline to run in the cloud, or on WAT.ai's supercomputer, Nebula.
- Fix management of secret keys and cloud credentials.
- Allow Spark to work across multiple nodes.
- Make the pipeline more idempotent. Some operations are breaking after being run once.
- Better logging during task execution.
- Create a staging area for processing the data during intermediate pipeline steps.
- Checkpoint which (timestamp, ss_id) pairs have been processed, so that they do not get processed twice, resulting in dupes.

### Sources:
I copied plenty of code and text form the following sources:
- [Setting up Airflow with Docker on Windows.](https://medium.com/@garc1a0scar/how-to-start-with-apache-airflow-in-docker-windows-902674ad1bbe)
- [openEDI data documentation](https://github.com/openEDI/documentation/blob/main/pvdaq.md)
- [Writing Docker Compose Files](https://www.techrepublic.com/article/how-to-build-a-docker-compose-file/)
- [Spark with Airflow](https://github.com/airscholar/SparkingFlow)
