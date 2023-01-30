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

def base_output_table(defhc_id, radius, start_date, end_date):
    """
    Function to return a one-rec spark df to join to all output tables to keep same columns on every table
    params:
        defhc_id int: input facility id
        radius int: input radius
        start_date str: input start_date
        end_date str: input end_date
        
    returns:
        one-rec spark df with above four columns
    
    """
    
    df = pd.DataFrame(data = {'defhc_id': defhc_id,
                             'radius': radius,
                             'start_date': start_date,
                             'end_date': end_date
                             },
                             index=[0]
                     )
    
    return spark.createDataFrame(df)    

# COMMAND ----------

def create_final_output(base_sdf, counts_sdf):
    """
    Function create_final_output to read in base and counts spark dfs to add base columns to counts, and add final time stamp
    params:
        base_sdf sdf: spark df to add BEFORE counts with input params
        counts_sdf sdf: sdf with all counts
        
    returns:
        spark df with base_sdf cols, counts_sdf cols, and current_dt
    """
    
    return base_sdf.join(counts_sdf).withColumn('current_dt', F.current_timestamp())

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
                             ])
    
    schema_dt = StructType([StructField('current_dt', TimestampType(), False)])
    
    # create measure columns from input dict
    
    schema_measures =  StructType([ StructField(msr_name, msr_type, False) for msr_name, msr_type in measure_dict.items() ])
    
    # combine all for final table and return empty spark dataframe
    
    schema_full = StructType([schema_base.fields + schema_measures.fields + schema_dt.fields][0])
    
    return spark.createDataFrame([], schema_full)

# COMMAND ----------

def insert_into_output(defhc_id, radius, start_date, end_date, sdf, table):
    """
    Function insert_into_output() to insert new measures data into table, 
        first checking if there are records existing for given id/radius/dates, and if so, deleting before insertion
        
    params:
        defhc_id int: input facility id
        radius int: input radius
        start_date str: input start_date
        end_date str: input end_date
        sdf spark df: spark df with records to be inserted (will insert all rows, all columns)
        table str: name of output table 
    
    returns:
        none, has optional deletion before insertion, will print deleted/inserted dates
    
    """
    
    # identify if same set of outputs exists in table - if so, print date and delete
    
    condition = f"""
        defhc_id = {defhc_id} and 
        radius = {radius} and 
        start_date = '{start_date}' and 
        end_date = '{end_date}'
        """
    
    old_dts = spark.sql(f"""
        select distinct current_dt
        from {table}
        where {condition}
        """).collect()
    
    if len(old_dts) > 0:
        print(f"Old records found, will delete: {', '.join([str(dt[0]) for dt in old_dts])}")
    else:
        print("No old records found to delete")
        
    spark.sql(f"""
        delete
        from {table}
        where {condition}
        """)
              
    # now insert new data, print all new records
              
    insert_cols = ', '.join(sdf.columns)
    sdf.createOrReplaceTempView('sdf_vw')
    
    spark.sql(f"""
        insert into {table} ({insert_cols})
        select * from sdf_vw
        """)
              
    print("New records inserted:")
              
    spark.sql(f"""
       select *
       from {table}
       where {condition}   
       """).display()

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
