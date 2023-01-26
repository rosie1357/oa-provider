# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include params for provider dashboard

# COMMAND ----------

BASEDIR = '/dbfs/FileStore/datascience/oa_provider'
S3_BUCKET = 'dhc-ize'
S3_KEY = 'oppurtunity-assesment/clientdata'

# COMMAND ----------

# link to physician page

PHYS_LINK = "https://www.defhc.com/physicians/"

# COMMAND ----------

# variables to hold names of base tables

MX_CLMS_TBL = 'mxclaims_master'
PCP_REFS_TBL = 'pcp_referrals'

# COMMAND ----------

GET_TMP_DATABASE = lambda x: f"{x}_tmp"

# COMMAND ----------

def network_flag(network_col, network_value, suffix=''):
    """
    Function network_flag() to create col network_flag based on network col name and literal value
    params:
        network_col str: name of column with network values to match against
        network_value int: value to match against to indicate in-network
        suffix str: optional param to add suffix to network_flag (if creating >1 per table, eg)
  
    """
    
    return f"""case when {network_col} = {network_value} then 'In-Network'
             else 'Out-of-Network'
             end as network_flag{suffix}
         """

# COMMAND ----------

def affiliated_flag(affiliation_col, affiliation_value, suffix=''):
    """
    Function affiliated_flag() to create col affiliated_flag based on affiliation ID column and literal value
    params:
        affiliation_col str: name of column with affiliation (defhc_id) values to match against
        affiliation_value int: value to match against to affiliated/competitor/independent
        suffix str: optional param to add suffix to affiliated_flag (if creating >1 per table, eg)
  
    """
    
    return f"""case when {affiliation_col} = {affiliation_value} then 'Affiliated'
               when {affiliation_col} is not null then 'Competitor'
               else 'Independent'
               end as affiliated_flag{suffix}
         """

# COMMAND ----------

def assign_fac_types(alias, current_col='FirmTypeName', new_col='facility_type'):
    """
    Function assign_fac_types to return sql text to create facility_type col from input firmtype
    params:
        alias str: alias for table to get FirmTypeName from
        current_col str: optional param for name of firm type existing column, default = FirmTypeName
        new_col str: optional param for name of new column, default = facility_type
    
    returns:
        sql case statement
    
    """
    
    return f"""
        case when {alias}.{current_col} in ('Ambulatory Surgery Center', 'Hospital', 'Imaging Center', 'Physician Group',
                                         'Renal Dialysis Facility', 'Retail Clinic', 'Urgent Care Clinic')
                                         
               then {alias}.{current_col}
               
               when {alias}.{current_col} in ('Assisted Living Facility', 'Home Health Agency', 'Hospice', 'Skilled Nursing Facility')
               then 'Post-Acute'
               
               else null
               end as {new_col}
       """
