# pv-pipeline

# What is it?
An ETL pipeline to extract and prepare photovoltaic output data (aka PV data) for ML model training and data analysis. The data is from the [Open Energy Data Initiative](https://data.openei.org/submissions/4568).

# Why does it exist?
I am working with [Open Climate Fix](https://openclimatefix.org/) on models that forecast the amount of energy a solar panel site will generate in the near future (aka PV nowcasting). One of the model inputs is historical PV data, the amount of energy a given solar panel site has generated in the past. The reasons for building this pipeline are:

- To expand our dataset. This pipeline extracts historical PV data from an S3 bucket, processes the data and stores it in a GCP storage bucket to be analyzed in BigQuery. This data will be used to train models soon.

- To show the data acquisition team at Cohere that I have basic data pipelining skills.

# How does it work?
- Airflow for task configuration and scheduling (although a cron job or even manual execution would suffice)
- PySpark as the data processing engine*
- BigQuery as the destination

(*) the power of PySpark is in parallel processing across multiple nodes. Without access to a multi-node machine or the bugdet to run multiple nodes in the cloud I am not truly leveraging PySpark. Running Dask on my PC would have been simpler and cheaper. But I am using PySpark as a learning exercise.


# Comments
### Was Airflow a good choice?
Not really. This pipeline does not run on a schedule and does not have complex dependencies between tasks, both things that Airflow does really well. However, I now have infrastructure to easily add other tasks that depend on each other. I can also easily monitor tasks statuses through the Airflow Web UI.

### Was PySpark a good choice?
Honest answer is I am not sure. This was my first time using PySpark and I am still understanding its use case. This pipeline will process over a terabyte of PV data. I imagine it can do that over a couple of days on my PC using Dask or even Pandas. However, if we had access to a multi-node computer and expected to process tons of data quickly, then having a Spark environment set up is beneficial.

### Was BigQuery a good choice?
Again, not sure. I do not have the budget to store and analyze all the data in BigQuery. I don't have a better alternative either. I will just have to analyze this data locally using SparkSQL or Dask. I used BigQuery as a learning exercise.

### I learned how to:
- extract data from an S3 bucket using boto3
- load data into BigQuery
  - configure a BigQuery table partition
  - design a database schema
- set up an Airflow environment
- use Spark
  - set up a Spark session
  - write PySpark code
