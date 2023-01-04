# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include params for provider dashboard

# COMMAND ----------

# variables to hold names of base tables

MX_CLMS_TBL = 'mxclaims_master'
PCP_REFS_TBL = 'pcp_referrals'
AFF_CLMS_TBL = 'affiliated_claims'

# COMMAND ----------

GET_TMP_DATABASE = lambda x: f"{x}_tmp"

# COMMAND ----------

def network_flag(network_col, network_value):
    """
    Function network_flag() to create col network_flag based on network col name and literal value
    params:
        network_col str: name of column with network values to match against
        network_value int: value to
  
    """
    
    return f"""case when {network_col} = {network_value} then 'In-Network'
             else 'Out-of-Network'
             end as network_flag
         """
