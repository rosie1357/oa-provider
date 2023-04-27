# Databricks notebook source
"""
Notebook to read in uploaded excel of most recent list of facilities to run and create
db table

NOTE!! Temporary process until standardized monthly runs are in place

"""

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

# MAGIC %run ../_funcs/_paths_include

# COMMAND ----------

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from _general_funcs.fs_funcs import pyspark_to_hive

from _funcs.params import REF_S3_BUCKET

# COMMAND ----------

# set params to use for input excel, end date, output table

IN_FILE = f"{REF_S3_BUCKET}/DashboardAlphaList_2023_03_28.xlsx"
END_DATE = '2022-11-30'
OUT_TABLE = 'ds_provider.provider_oa_inputs_20230328'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 0. Functions

# COMMAND ----------

def get_start_date(timeframe, end_date, date_format='%Y-%m-%d'):
    """
    Function get_start_date to apply to df and return string value for start date
    params:
        timeframe str: df Timeframe col where final part is # of months to subtract from end date
        end_date str: end date to use to create start date from
        date_format str: optional param to specify date format for input/output dates, default = '%Y-%m-%d'
        
    returns:
        str value for start date
    
    """
    
    # extract months from timeframe col
    months = int(timeframe.split(' ')[-1])
    
    # put end date into date format, then create start date by subtracting # of months and adding 1 day  
    
    end_dt = datetime.strptime(end_date, date_format)
    start_dt = end_dt - relativedelta(months=months) + relativedelta(months=1, day=1)

    # convert to string format to return
    
    return start_dt.strftime(date_format)

# COMMAND ----------

def create_bulk_table(infile, end_date):
    """
    function create_bulk_table to read in excel with expected cols and reformat/create
        cols needed for bulk runs table
        
    params:
        infile str: full path to xlsx
        end_date str: end date to use for all entries, will then create start date based on Timeframe col with # of months to use
        
    returns:
        df with columns to create perm db table with runs info    
    
    """
    
    df = pd.read_excel(infile)
    df['SORT'] = df.index
    
    # drop trailing recs at end (null ID)

    df = df.dropna(subset=['DefHCID']).reset_index(drop=True)
    
    # reformat/recreate cols to fit table layout
    
    df[['DEFHC_ID', 'RADIUS']] = df[['DefHCID', 'Radius']].applymap(lambda x: str(int(x)))
    df['LT18'] = df['Under 18'].apply(lambda x: 1 if x == 1 else 0)
    df['END_DATE'] = end_date
    
    # call get_start_date to create start date based on timeframe (number of preceding months)
    
    df['START_DATE'] = df['Timeframe'].apply(get_start_date, end_date=end_date)
    
    return df[['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'LT18', 'SORT']]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Read Excel list

# COMMAND ----------

df = create_bulk_table(infile = IN_FILE,
                       end_date = END_DATE)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Create perm table

# COMMAND ----------

# using df created above, create perm table to use for bulk runs

pyspark_to_hive(spark.createDataFrame(df),
                OUT_TABLE)

# COMMAND ----------

# display new table

spark.sql(f"select * from {OUT_TABLE} order by SORT").display()
