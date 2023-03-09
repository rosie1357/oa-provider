# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include output funcs for provider dashboard

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/aws_funcs

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/fs_funcs

# COMMAND ----------

import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# COMMAND ----------

def create_empty_output(measure_dict):
    """
    Function create_empty_output to read in dictionary of measure name and type to sandwich between base cols and final time stamp col
    params:
        measure_dict dictionary: dictionary of string name and measure type, eg '{'npi': IntegerType()}'

    returns:
        empty spark df with given fields
        
    """
    
    # create set of initial base columns and final time stamp column
    
    schema_base = StructType([ \
                              StructField('defhc_id', IntegerType(), False), \
                              StructField('radius', IntegerType(), False), \
                              StructField('start_date', StringType(), False), \
                              StructField('end_date', StringType(), False), \
                              StructField('subset_lt18', IntegerType(), False), \
                             ])
    
    schema_dt = StructType([StructField('current_dt', TimestampType(), False)])
    
    # create measure columns from input dict
    
    schema_measures =  StructType([ StructField(msr_name, msr_type, False) for msr_name, msr_type in measure_dict.items() ])
    
    # combine all for final table and return empty spark dataframe
    
    schema_full = StructType([schema_base.fields + schema_measures.fields + schema_dt.fields][0])
    
    return spark.createDataFrame([], schema_full)

# COMMAND ----------

def csv_upload_s3(table, bucket, key_prefix, **cred_kwargs):
    """
    Function csv_upload_s3 to upload given hive table as csv to s3
    params:
        table str: name of hive table, format of db.tablename
        bucket str: name of bucket to upload to
        key_prefix str: key within bucket to upload to
        **cred_kwargs: aws credentials, with aws_access_key_id/aws_secret_access_key, optional aws_session_token
        
    returns:
        none, uploads file as csv    
    
    """
    
    # create client to connect using creds
    
    client = boto3_s3_client(**cred_kwargs)
        
    # get name of table (without database name), read in from hive and then convert to csv
    
    name = table.split('.')[-1]
    out_file = f"/tmp/{name}.csv"

    df = hive_to_df(table, df_type='pandas')
    df.to_csv(out_file, index=False)
    
    # upload to s3

    upload_s3(client, bucket, key_prefix, out_file)      
