# sj_dev
This solution uses a Medallion Architecture (bronze,silver,gold) implemented in databrciks with pyspark and delta lake.
It leverages Auto Loader for scalable ingestion and DLT concepts for data quality.

Design Overview:
1. Bronze (Raw): Ingestion layer using Auto loader.It considers schema evolution, to capture all raw data without failing.Also in this scenario ,csvs are appended daily so we do not need cluster to be active all time. Hence trigger availableNow is used.
2.Silver:Handles data quality and filter out unexpected values for power output column.Schema enforcement used to avoid surprises at gold layer.Here we can implement "Quarantine" pattern to capture those new columns and manually evolve after review.In silver layer, I am using trigger as AvailableNow. As here we are using forward-fill window function ,this trigger ensures that spark can look across the entire newly arrived daily dataset to calculate forward fill correctly.It also ensures the silver stays synchronised with daily bronze ingestion. 
Gold Layer: Strict schema.No trigger.And this calculates symmery statistics over 24-hour window.