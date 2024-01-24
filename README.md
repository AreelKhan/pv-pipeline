# pv-pipeline

This a brief version of a write up for a data pipeline I am building. To find the detailed vesion click here.

# What is this?
An ETL pipeline to extract and prepare photovoltaic output data (aka PV data) for ML model training and data analysis. The data is from the [Open Energy Data Initiative](https://data.openei.org/submissions/4568).

# Why does it exist?
I am working with [Open Climate Fix](https://openclimatefix.org/), a not-for-profit using AI to fight climate change. We are working on models that forecast the amount of energy a solar panel site will generate in the near future (aka PV nowcasting).

One of the model inputs is historical PV data, the amount of energy a given solar panel site has generated in the past.

To expand our dataset, I am building this data pipeline that extracts historical PV data from an S3 bucker, processes the data and stores it in a google cloud storage bucket. This data will be used to train models soon.

The second reason to build this pipeline is to show the data acquisition team at Cohere that I have basic data pipelining skills.

# How does it work?
- Tasks are configured and scheduled using Airflow (although a cron job or even manual execution suffices)
- PySpark is the data processing engine*
- Pipeline runs on GCP compute engine.
- Output data is stored in GCP storage buckets.

(*) the power of PySpark is in parallel processing across multiple nodes. Without the bugdet to run multiple nodes in the cloud I am not truly leveraging PySpark. Running Dask on my PC would have been simpler and cheaper.

### Data flow:

I intially planned on building this pipeline much later (in May) using just Dask and running it on my PC or WAT.ai's super computer cluster, but as mini-project for Cohere, I am building it now, and utilizing Airflow and PySpark on Google Cloud (a tad overengineered but good learning experience).


# Comments
### Was Airflow a good choice?
Not at all. Airflow was overkill. This task does not need to run on a schedule and does not depend on upstream tasks. It can run as a cron job or even a manually executed task. I do however have infrastructure to very easily add other tasks now.

### Was PySpark a good choice?
Honest answer is I am not sure. This pipeline will process over a terabyte of PV data. I imagine it can do that over a couple of days on my PC using Dask. However, if we had access to a multi-node computer and expected to process tons of data, then having a Spark environment set up is beneficial.

### Was GCP a good choice?
Again, not sure. Since I don't have the budget to pay cloud fees, I would much prefer running this pipeline on my PC or WAT.ai's supercomputer cluster (which is in fact what I'll do when I need to run this pipeline in the future). The goal was to use Google Cloud even if it was not the optimal solution.

### What I learned:
- How to extract data from an S3 bucket using boto3.
