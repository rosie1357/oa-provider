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

def base_output_table(defhc_id, radius, start_date, end_date, subset_lt18, id_prefix=''):
    """
    Function to return a one-rec spark df to join to all output tables to keep same columns on every table
    params:
        defhc_id int: input facility id
        radius int: input radius
        start_date str: input start_date
        end_date str: input end_date
        subset_lt18 int: indicator for subset to lt18, = 0 or 1
        id_prefix str: optional param to specify prefix on defhc_id (used for initial table creation, will be input_defhc_id), default=''
        
    returns:
        one-rec spark df with above four columns
    
    """
    
    df = pd.DataFrame(data = {f"{id_prefix}defhc_id": defhc_id,
                             'radius': radius,
                             'start_date': start_date,
                             'end_date': end_date,
                             'subset_lt18': subset_lt18
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
                              StructField('subset_lt18', IntegerType(), False), \
                             ])
    
    schema_dt = StructType([StructField('current_dt', TimestampType(), False)])
    
    # create measure columns from input dict
    
    schema_measures =  StructType([ StructField(msr_name, msr_type, False) for msr_name, msr_type in measure_dict.items() ])
    
    # combine all for final table and return empty spark dataframe
    
    schema_full = StructType([schema_base.fields + schema_measures.fields + schema_dt.fields][0])
    
    return spark.createDataFrame([], schema_full)

# COMMAND ----------

def populate_most_recent(sdf, table, condition):
    """
    Function populate_most_recent to identify any recs in given table to set as most_recent=False before inserting recent recs
    params:
        sdf spark df: sdf to insert (without most_recent col)
        table str: name of table to update
        condition str: where stmt (without leading 'where') to identify old recs to set as most_recent=False
        
    returns:
        none
    """
    
    # first, identify if there are any existing counts for the same condition/table, and set most_recent = False
    
    spark.sql(f"update {table} set most_recent = False where {condition}")
    
    # get cols to populate in perm table
    
    tbl_cols = hive_tbl_cols(table)
    
    # create view and insert into table
    
    sdf.createOrReplaceTempView('sdf_vw')
    
    spark.sql(f"""
    insert into {table} ({insert_cols})
    select *, True as most_recent from sdf_vw
    """)
    

# COMMAND ----------

def insert_into_output(defhc_id, radius, start_date, end_date, subset_lt18, counts_table, sdf, table, must_exist=True, maxrecs=25, id_prefix=''):
    """
    Function insert_into_output() to insert new data into table, 
        first checking if there are records existing for given id/radius/dates, and if so, deleting before insertion
        
    params:
        defhc_id int: input facility id
        radius int: input radius
        start_date str: input start_date
        end_date str: input end_date
        subset_lt18 int: indicator for subset lt18
        counts_table str: name of table to insert count of records into
        sdf spark df: spark df with records to be inserted (will insert all rows, all columns)
        table str: name of output table 
        must_exist bool: optional param to specify table must exist before run, default = True
            if not, will create if does NOT exist and will print message that created
        maxrecs int: optional param to specify max recs to display, default = 25
        id_prefix str: optional param to specify prefix on defhc_id (used for initial table creation, will be input_defhc_id), default=''
    
    returns:
        count of inserted records
        has optional deletion before insertion, will print deleted/inserted dates
    
    """            
    
    # identify if same set of outputs exists in table - if so, print date and delete
    
    condition = f"""
        {id_prefix}defhc_id = {defhc_id} and 
        radius = {radius} and 
        start_date = '{start_date}' and 
        end_date = '{end_date}' and
        subset_lt18 = {subset_lt18}
        """
    
    # create view from sdf
    
    sdf.createOrReplaceTempView('sdf_vw')
    
    # only run commands to select/delete from given table IF table must exist OR must not exist but already exists
    
    database, table_name = table.split('.')[0], table.split('.')[1]

    table_exists = spark._jsparkSession.catalog().tableExists(database, table_name)
    
    if must_exist or (must_exist==False and table_exists):

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
        
        spark.sql(f"""
            insert into {table} ({insert_cols})
            select * from sdf_vw
            """)
    else:
        
        print(f"Table {table} did NOT exist - creating now")
        
        spark.sql(f"""
            create table {table} as 
            select * from sdf_vw        
            """)
        
    print(f"New records inserted (max {maxrecs}):")
              
    spark.sql(f"""
       select *
       from {table}
       where {condition}
       limit {maxrecs}
       """).display()
    
    tbl_count = hive_tbl_count(table, condition = f"where {condition}")
    
    # create sdf with base cols, count and time stamp, then call populate_counts() to set any remaining recs to False and insert new rec
    
    counts_sdf = create_final_output(base_sdf = base_output_table(defhc_id, radius, start_date, end_date, subset_lt18), 
                                     counts_sdf = spark.createDataFrame(pd.DataFrame(data={'database': database,
                                                                                           'table_name': table_name,
                                                                                           'count': tbl_count}, index=[0]))
                                    )
    
    populate_most_recent(sdf = counts_sdf,
                         table = counts_table,
                         condition = f"{condition} and database = '{database}' and table_name = '{table_name}'")
    
    return tbl_count

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
