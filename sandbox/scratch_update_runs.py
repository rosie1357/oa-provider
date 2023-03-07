# Databricks notebook source
"""
scratch notebook to backfill info for prior runs before implementing additional features
    1. add subset_lt18 columns and fill with given facility's value
    2. populate counts table
    3. populate run status table

"""

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Set table name/run values params

# COMMAND ----------

# get list of ALL 'page' tables

all_tables = spark.sql("show tables in ds_provider").collect()
page_tables = [t[1] for t in all_tables if t[1].startswith('page')]

page_tables

# COMMAND ----------

# create list of ID-specific tables in each facility's database

db_tables = ['input_org_info','nearby_hcos_id','nearby_hcps','nearby_hcos_npi',
             f"{MX_CLMS_TBL}",f"{PCP_REFS_TBL}",'inpat90_dashboard','inpat90_facilities']

# COMMAND ----------

# read in input table to get all unique values for ID and LT18 (will confirm each ID only has one value for LT18)

INPUT_TABLE = 'ds_provider.provider_oa_inputs_v1'

INPUTS = spark.sql(f"""select distinct DEFHC_ID, LT18 from {INPUT_TABLE}""")
UNIQUE_IDS = spark.sql(f"""select distinct DEFHC_ID from {INPUT_TABLE}""")

INPUTS.count(), UNIQUE_IDS.count()

# COMMAND ----------

# put into pandas df, add MGH (one additional facility not in input list)

RUNS_DF = pd.concat([INPUTS.toPandas(), pd.DataFrame(data={'DEFHC_ID': 1973, 'LT18': '0'}, index=[0])]).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Create/fill LT18 columns

# COMMAND ----------

# create statement to add empty col on individual database table at needed position, and to fill with given value

def create_subset_col(id, table, subset_lt18, page_table=False):
    
    db = "ds_provider"
    match_col = 'defhc_id'

    if page_table == False:
        
        db = f"ds_provider_{id}"
        match_col = "input_defhc_id"
        
        # skip if column already exists (if rerunning to avoid inserting more than once)
        
        if not 'subset_lt18' in hive_tbl_cols(f"{db}.{table}", as_list=True):
        
            spark.sql(f"alter table {db}.{table} add column subset_lt18 integer after end_date")
    
    spark.sql(f"update {db}.{table} set subset_lt18 = {subset_lt18} where {match_col} = '{id}'")

# COMMAND ----------

for index, row in RUNS_DF.iterrows():

    defhc_id, subset_lt18 = row['DEFHC_ID'], row['LT18']
    print(f"{index+1}. ID = {defhc_id}, subset = {subset_lt18}")
    
    for page_table in page_tables:

         create_subset_col(id = defhc_id, 
                           table = page_table, 
                           subset_lt18 = int(subset_lt18),
                           page_table=True)

    for db_table in db_tables:

        create_subset_col(id = defhc_id, 
                           table = db_table, 
                           subset_lt18 = int(subset_lt18))
