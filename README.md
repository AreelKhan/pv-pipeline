# pv-pipeline

Hey JT!

Hope you've been well. I appreciate the opportunity to interview for your team. It was great meeting Anson!

Tbh I don't think I did well on my interview since I tend to get quite nervous. Instead, I built a project (an ETL pipeline in GCP, Airflow and PySpark) to show that I do have the basic skills you are looking for. Here is a brief write up and repo. Please take a look if you get a chance and let me know what you think!





Hey! This a brief version of a write up for a data pipeline I am building. To find the detailed vesion click here.

What is this?
An ETL pipeline to extract and prepare photovoltaic output data (aka PV data) for ML model training and data analysis. The data is from the Canadian government website

Why does it exist?
I am working with Open Climate Fix, a not-for-profit using AI to fight climate change. We are working on models that forecast the amount of energy a solar panel site will generate in the near future (aka PV nowcasting).

One of the model inputs is historical PV data, the amount of energy a given solar panel site has generated in the past.

We want to expand our dataset to include data about sites in Canada. This data pipeline extracts historical PV data from the Canadian government website about canadian solar panel sites. This data will be used to train models soon.

The second reason to build this pipeline is to show the data acquisition team at Cohere that I have basic data pipelining skills.

Since these two goals align, I decided to kill two birds with one stone and build this.


How does it work?
