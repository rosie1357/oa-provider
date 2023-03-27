# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 004 Specialists
# MAGIC 
# MAGIC **Program:** 004_specialists
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider specialists page <br>
# MAGIC <br>
# MAGIC **NOTE**: DATABASE param below is value extracted from database widget, FAC_DATABASE is assigned in ProviderRunClass
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {FAC_DATABASE}.input_org_info
# MAGIC   - {FAC_DATABASE}.nearby_hcps
# MAGIC   - {FAC_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {FAC_DATABASE}.{PCP_REFS_TBL}
# MAGIC   
# MAGIC **Outputs** (inserted into):
# MAGIC   - {DATABASE}.page3_top_panel_specialists
# MAGIC   - {DATABASE}.page3_shares
# MAGIC   - {DATABASE}.page3_top_pcp_flow

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

# create all widgets
 
RUN_VALUES = get_widgets()

# COMMAND ----------

# get widget values and use to create instance of provider run class

DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'SUBSET_LT18', 'DATABASE', 'RUN_QC'])

ProvRunInstance = ProviderRun(DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, DATABASE, RUN_QC)

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
           ,affiliation_2cat
           
           ,sum(case when network_flag = 'In-Network' then 1 else 0 end) as count_in_network
           ,sum(case when network_flag = 'Out-of-Network' then 1 else 0 end) as count_out_of_network
           ,sum(case when network_flag = 'No Network' then 1 else 0 end) as count_no_network
           
           ,sum(case when network_flag = 'In-Network' and {HOSP_ASC_HOPD_SUBSET} then 1 else 0 end) as count_in_network_hosp_asc
           ,sum(case when network_flag = 'Out-of-Network' and {HOSP_ASC_HOPD_SUBSET} then 1 else 0 end) as count_out_of_network_hosp_asc
           ,sum(case when network_flag = 'No Network' and {HOSP_ASC_HOPD_SUBSET} then 1 else 0 end) as count_no_network_hosp_asc
           
   from mxclaims_master_vw
   where specialty_type = 'Specialist'
   group by RenderingProviderNPI
           ,ProviderName
           ,rendering_npi_url
           ,specialty_cat
           ,affiliation_2cat
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page3_top_panel_specialists"

ProvRunInstance.create_final_output(page3_top_sdf, table=TBL_NAME)

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
           ,affiliation_2cat
           ,pos_cat as place_of_service
           ,network_flag
           ,count(*) as count
           
   from mxclaims_master_vw
   where {HOSP_ASC_HOPD_SUBSET} and 
         specialty_type = 'Specialist'
         
   group by net_defhc_id
           ,net_defhc_name
           ,specialty_cat
           ,affiliation_2cat
           ,pos_cat
           ,network_flag
         
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, upload to s3

TBL_NAME = f"{DATABASE}.page3_shares"

ProvRunInstance.create_final_output(page3_shares_sdf, table=TBL_NAME)

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
           ,affiliation_2cat_spec
           ,network_flag_spec
           ,count(*) as count
           
    from pcp_referrals_vw
          
    group by npi_pcp
           ,name_pcp
           ,npi_url_pcp
           ,npi_spec
           ,name_spec
           ,npi_url_spec
           ,specialty_cat_spec
           ,affiliation_spec
           ,affiliation_2cat_spec
           ,network_flag_spec

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, upload to s3

TBL_NAME = f"{DATABASE}.page3_top_pcp_flow"

ProvRunInstance.create_final_output(page3_top_pcp_flow_sdf, table=TBL_NAME)

# COMMAND ----------

ProvRunInstance.exit_notebook()
