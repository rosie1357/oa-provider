# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 005 PCPs
# MAGIC 
# MAGIC **Program:** 005_pcps
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider PCP page <br>
# MAGIC <br>
# MAGIC **NOTE**: DATABASE and FAC_DATABASE params below are value extracted from database widget, value passed to GET_FAC_DATABASE() lambda func param, tbl var names specified in params
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {FAC_DATABASE}.input_org_info
# MAGIC   - {FAC_DATABASE}.{PCP_REFS_TBL}
# MAGIC   
# MAGIC **Outputs** (inserted into):
# MAGIC   - {DATABASE}.page4_loyalty_map_pcps
# MAGIC   - {DATABASE}.page4_pcp_dist
# MAGIC   - {DATABASE}.page4_patient_flow_pcps
# MAGIC   - {DATABASE}.page4_net_leakage

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

from functools import partial

# COMMAND ----------

# setup: 
#  create/get widget values, assign fac database and network, create views

RUN_VALUES = get_widgets()

DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'SUBSET_LT18', 'DATABASE', 'RUN_QC'])

FAC_DATABASE = GET_FAC_DATABASE(DATABASE, DEFHC_ID)

create_views(DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, FAC_DATABASE, ALL_TABLES, id_prefix='input_')

INPUT_NETWORK, DEFHC_NAME = sdf_return_row_values(hive_to_df('input_org_info_vw'), ['input_network', 'defhc_name'])

# create dictionary of counts to fill in for each table insert and return on pass

COUNTS_DICT = {}

# COMMAND ----------

# create base df to create partial for create_final_output function

base_sdf = base_output_table(DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18)
create_final_output_func = partial(create_final_output, base_sdf)

# create partial for insert_into_output function

insert_into_output_func = partial(insert_into_output, DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18)

# create partial for save to s3

upload_to_s3_func = partial(csv_upload_s3, bucket=S3_BUCKET, key_prefix=S3_KEY, **AWS_CREDS)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. PCP Loyalty by Zip Code Map

# COMMAND ----------

# aggregate claims by specialty and PCP zip code, getting count in/out of network

page4_loyalty_map_sdf = spark.sql(f"""
    select specialty_cat_spec
        ,  zip_pcp as zipcd
        ,  sum(case when network_flag_spec = 'In-Network' then 1 else 0 end) as count_in_network
        ,  sum(case when network_flag_spec = 'Out-of-Network' then 1 else 0 end) as count_out_of_network
        
    from pcp_referrals_vw
          
    group  by specialty_cat_spec
        ,  zip_pcp 
           
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page4_loyalty_map_pcps"

page4_loyalty_map = create_final_output_func(page4_loyalty_map_sdf)

COUNTS_DICT[TBL_NAME] = insert_into_output_func(page4_loyalty_map.sort('specialty_cat_spec', 'zipcd'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. PCP Distribution

# COMMAND ----------

page4_pcp_dist_sdf = spark.sql(f"""
    select npi_pcp
        ,  specialty_cat_spec
        ,  affiliation_4cat_pcp
        ,  sum(case when network_flag_spec = 'In-Network' then 1 else 0 end) as count_in_network
        ,  sum(case when network_flag_spec = 'Out-of-Network' then 1 else 0 end) as count_out_of_network
           
    from   pcp_referrals_vw
         
   group   by npi_pcp
       ,   specialty_cat_spec
       ,   affiliation_4cat_pcp
         
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{DATABASE}.page4_pcp_dist"

page4_pcp_dist = create_final_output_func(page4_pcp_dist_sdf)

COUNTS_DICT[TBL_NAME] = insert_into_output_func(page4_pcp_dist.sort('npi_pcp', 'specialty_cat_spec'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Top 10 PCPs; Patient Flow

# COMMAND ----------

# read in referrals and aggregate 

page4_patient_flow_pcps_sdf = spark.sql(f"""
    
    select npi_pcp
        ,  name_pcp
        ,  npi_url_pcp
        ,  specialty_cat_spec
        ,  npi_spec
        ,  name_spec
        ,  npi_url_spec
        ,  network_flag_spec
        ,  count(*) as count
           
    from   pcp_referrals_vw
          
    group by npi_pcp
        ,  name_pcp
        ,  npi_url_pcp
        ,  specialty_cat_spec
        ,  npi_spec
        ,  name_spec
        ,  npi_url_spec
        ,  network_flag_spec

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{DATABASE}.page4_patient_flow_pcps"

page4_patient_flow_pcps = create_final_output_func(page4_patient_flow_pcps_sdf)

COUNTS_DICT[TBL_NAME] = insert_into_output_func(page4_patient_flow_pcps.sort('npi_pcp', 'specialty_cat_spec'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md ### 4. Top 10 Leakage Networks

# COMMAND ----------

# read in referrals and aggregate 

page4_net_leakage_sdf = spark.sql(f"""
    
    select specialty_cat_spec
        ,  net_defhc_id_spec
        ,  net_defhc_name_spec
        ,  count(*) as count
        
    from   pcp_referrals_vw
    where  net_defhc_id_pcp = {INPUT_NETWORK}
    and    net_defhc_id_spec != {INPUT_NETWORK}
    and    net_defhc_id_spec is not null
    
    group  by specialty_cat_spec
        ,  net_defhc_id_spec
        ,  net_defhc_name_spec

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{DATABASE}.page4_net_leakage"

page4_net_leakage = create_final_output_func(page4_net_leakage_sdf)

COUNTS_DICT[TBL_NAME] = insert_into_output_func(page4_net_leakage.sort('specialty_cat_spec'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

exit_notebook({'all_counts': COUNTS_DICT},
              fail=False)
