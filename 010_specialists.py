# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 010 Specialists
# MAGIC 
# MAGIC **Program:** 010_specialists
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider specialists page <br>
# MAGIC <br>
# MAGIC **NOTE**: DATABASE and TMP_DATABASE params below are value extracted from database widget, value passed to TMP_DATABASE() lambda func param, tbl var names specified in params
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {TMP_DATABASE}.input_org_info
# MAGIC   - {TMP_DATABASE}.nearby_hcps
# MAGIC   - {TMP_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {TMP_DATABASE}.{PCP_REFS_TBL}
# MAGIC   - MartDim.D_Organization
# MAGIC   - MxMart.F_MxClaim_v2
# MAGIC   
# MAGIC **Outputs** (inserted into):

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

from functools import partial

# COMMAND ----------

# setup: 
#  create/get widget values, assign temp database and network

RUN_VALUES = get_widgets()

DEFHC_ID, RADIUS, START_DATE, END_DATE, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'DATABASE', 'RUN_QC'])

TMP_DATABASE = GET_TMP_DATABASE(DATABASE)

INPUT_NETWORK = sdf_return_row_values(hive_to_df(f"{TMP_DATABASE}.input_org_info"), ['input_network'])

# COMMAND ----------

# create base df to create partial for create_final_output function

base_sdf = base_output_table(DEFHC_ID, RADIUS, START_DATE, END_DATE)
create_final_output_func = partial(create_final_output, base_sdf)

# create partial for insert_into_output function

insert_into_output_func = partial(insert_into_output, DEFHC_ID, RADIUS, START_DATE, END_DATE)

# create partial for save to s3

upload_to_s3_func = partial(csv_upload_s3, bucket=S3_BUCKET, key_prefix=S3_KEY, **AWS_CREDS)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Top Panel; Top 10 Specialists

# COMMAND ----------

# subset claims to specialists only, aggregate claim counts for output table

page3_top_sdf = spark.sql(f"""
    select RenderingProviderNPI as npi
           ,ProviderName as name
           ,rendering_npi_url as npi_url
           ,specialty_cat
           ,affiliated_flag
           ,sum(case when network_flag = 'In-Network' then 1 else 0 end) as count_in_network
           ,sum(case when network_flag = 'Out-of-Network' then 1 else 0 end) as count_out_of_network
           
   from {TMP_DATABASE}.{MX_CLMS_TBL}
   where specialty_type = 'Specialist'
   group by RenderingProviderNPI
           ,ProviderName
           ,rendering_npi_url
           ,specialty_cat
           ,affiliated_flag
           
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page3_top_panel_specialists"

page3_top_panel_specialists = create_final_output_func(page3_top_sdf)

insert_into_output_func(page3_top_panel_specialists.sort('npi', 'specialty_cat', 'affiliated_flag'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Hospital and HOPD/ASC Shares

# COMMAND ----------

# read in claims and subset to specialist, Inpatient Hospital and HOPD/ASC, and get aggregates for AW

page3_shares_sdf = spark.sql(f"""
    select net_defhc_id
           ,net_defhc_name
           ,specialty_cat
           ,affiliated_flag
           ,pos_cat
           ,network_flag
           ,count(*) as count
           
   from {TMP_DATABASE}.{MX_CLMS_TBL}
   where specialty_type = 'Specialist' and 
         pos_cat in ('ASC & HOPD', 'Hospital Inpatient')
         
   group by net_defhc_id
           ,net_defhc_name
           ,specialty_cat
           ,affiliated_flag
           ,pos_cat
           ,network_flag
         
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, upload to s3

TBL_NAME = f"{DATABASE}.page3_shares"

page3_shares = create_final_output_func(page3_shares_sdf)

insert_into_output_func(page3_shares.sort('net_defhc_id', 'net_defhc_name', 'specialty_cat'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Top 10 PCPs; Patient Flow

# COMMAND ----------

# read in referrals and aggregate 

page3_top_pcp_flow_sdf = spark.sql(f"""
    
    select npi_pcp
           ,name_pcp
           ,npi_url_pcp
           ,npi_spec
           ,name_spec
           ,npi_url_spec
           ,specialty_cat_spec
           ,affiliation_spec
           ,affiliated_flag_spec
           ,network_flag_spec
           ,count(*) as count
           
    from {TMP_DATABASE}.{PCP_REFS_TBL}
    group by npi_pcp
           ,name_pcp
           ,npi_url_pcp
           ,npi_spec
           ,name_spec
           ,npi_url_spec
           ,specialty_cat_spec
           ,affiliation_spec
           ,affiliated_flag_spec
           ,network_flag_spec

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, upload to s3

TBL_NAME = f"{DATABASE}.page3_top_pcp_flow"

page3_top_pcp_flow = create_final_output_func(page3_top_pcp_flow_sdf)

insert_into_output_func(page3_top_pcp_flow.sort('npi_pcp', 'npi_spec'), TBL_NAME)

upload_to_s3_func(TBL_NAME)
