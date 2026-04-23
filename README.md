# sj_dev

**Consider the following scenario:**
You are a data engineer for a renewable energy company that operates a farm of wind turbines.
The turbines generate power based on wind speed and direction, and their output is measured
in megawatts (MW). Your task is to build a data processing pipeline that ingests raw data from
the turbines and performs the following operations:
● Cleans the data: The raw data contains missing values and outliers, which must be
removed or imputed.
● Calculates summary statistics: For each turbine, calculate the minimum, maximum, and
average power output over a given time period (e.g., 24 hours).
● Identifies anomalies: Identify any turbines that have significantly deviated from their
expected power output over the same time period. Anomalies can be defined as turbines
whose output is outside of 2 standard deviations from the mean.
● Stores the processed data: Store the cleaned data and summary statistics in a database
for further analysis.
Data is provided to you as CSVs which are appended daily. Due to the way the turbine
measurements are set up, each csv contains data for a group of 5 turbines. Data for a particular
turbine will always be in the same file (e.g. turbine 1 will always be in data_group_1.csv). Each
day the csv will be updated with data from the last 24 hours, however the system is known to
sometimes miss entries due to sensor malfunctions.

Design Solution:
This solution uses a Medallion Architecture (bronze,silver,gold) implemented in databrciks with pyspark and delta lake.
It leverages Auto Loader for scalable ingestion and DLT concepts for data quality.

**- Design Overview:**
1. Bronze (Raw): Ingestion layer using Auto loader.It considers schema evolution, to capture all raw data without failing.Also in this scenario ,csvs are appended daily so we do not need cluster to be active all time. Hence trigger availableNow is used.
2.Silver:Handles data quality and filter out unexpected values for power output column.Schema enforcement used to avoid surprises at gold layer.Here we can implement "Quarantine" pattern to capture those new columns and manually evolve after review.In silver layer, I am using trigger as AvailableNow. As here we are using forward-fill window function ,this trigger ensures that spark can look across the entire newly arrived daily dataset to calculate forward fill correctly.It also ensures the silver stays synchronised with daily bronze ingestion. 
Gold Layer: Strict schema.No trigger.And this calculates symmery statistics over 24-hour window.

**Unit Testing**
- Finally to test this notebook locally, added one test cell for unit testing purpose.