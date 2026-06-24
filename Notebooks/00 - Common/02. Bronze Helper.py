# Databricks notebook source
# Helper function to add source_file and timestamp metadada
from pyspark.sql import functions as f

def add_metadata(df):
    return (
        df
        .withColumn('ingestion_timestamp', f.current_timestamp())
        .withColumn('source_file', f.col('_metadata.file_path'))
                  
    )