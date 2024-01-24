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
I intially planned on building this pipeline much later (in May) using just Dask and running it on my PC or WAT.ai's super computer cluster, but as mini-project for Cohere, I am building it now, and utilizing Airflow and PySpark on Google Cloud (a tad overengineered but good learning experience).

### Data flow:
