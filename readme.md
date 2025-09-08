# Overview
The goal of my project is to allow supermarket customers to spend as little as possible on grocery products. Therefore, we will extract products, prices, and other information from a market and maintain a history of these products. Our customers will have a dashboard where they can search for products and see how the price is changing over time, minimums and maximums, and the current price.

![Logo](.github/src/img/overview.png)

# To do
- Create dashboard visualization
- Create windows routine to start airflow

# Done
- Create script to adjust json files to be extracted on our operation.etl_extract
- Create "market_001" load to database script
- Create table to manage extract->load executions
- Create database 
- Create "market_001" extraction to json
- Create airflow docker-composer file
- Create DAG to execute every day our extraction


# Start airflow
cd airflow
docker compose up airflow-best-price --build