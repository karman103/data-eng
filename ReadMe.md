# Intro
This is a data engineering project that extracts data from yfinace api , converts the extracted json to avro and dumps it in Google Cloud Storage. After that it transfers the data to Bigquery. For data orchestration we use airflow.
# Source
This project is heavily influenced by this [article](https://medium.com/@rivaldi52/build-stock-price-data-etl-pipeline-using-python-airflow-and-google-cloud-service-c9726d6ee83b)
# Future works
For future I may use the [alpaca api](https://alpaca.markets/data) instead of yfinace api to adjust the usage of realtime data and for shorter time frames (seconds, minutes etc.) that are  not offered with yfinance api
