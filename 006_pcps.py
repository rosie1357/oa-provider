# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 006 PCPs
# MAGIC 
# MAGIC **Program:** 006_pcps
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider PCP page <br>
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
# MAGIC 
# MAGIC 
# MAGIC *Outstanding questions:*
# MAGIC 1. *should this page be limited to ASC/HOPD/Hospital Inpatient?*
# MAGIC 2. *confirm PCP Distribution should be count of providers*

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
# MAGIC ### 1. PCP Loyalty by Zip Code Map

# COMMAND ----------

# subset claims to specialists only, aggregate claim counts for output table

page4_loyalty_map_sdf = spark.sql(f"""
    select specialty_cat_spec
        ,  zipcd  
        ,  sum(case when network_flag_spec = 'In-Network' then 1 else 0 end) as count_in_network
        --,  count(distinct case when network_flag_spec = 'In-Network' then patient_id end) as pats_in_network
        ,  sum(case when network_flag_spec = 'Out-of-Network' then 1 else 0 end) as count_out_of_network
        --,  count(distinct case when network_flag_spec = 'Out-of-Network' then Patient_Id end) as pats_out_of_network
    from   {TMP_DATABASE}.{PCP_REFS_TBL} a 
    join   MartDim.D_Provider b 
    on     a.npi_pcp = b.npi 
    where  specialty_type_pcp = 'PCP' 
    and    specialty_type_spec = 'Specialist'
    and    rend_pos_cat in ('ASC & HOPD', 'Hospital Inpatient') 
    group  by specialty_cat_spec
        ,  zipcd 
           
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page4_loyalty_map_pcps"

page4_loyalty_map = create_final_output_func(page4_loyalty_map_sdf)

insert_into_output_func(page4_loyalty_map.sort('specialty_cat_spec', 'zipcd'), TBL_NAME)

# upload_to_s3_func(TBL_NAME)

# page4_loyalty_map.sort('specialty_cat_spec', 'zipcd').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. PCP Distribution

# COMMAND ----------

page4_pcp_dist_sdf = spark.sql(f"""
    with t1 as 
    (
    select npi_pcp
        ,  specialty_cat_spec
        ,  affiliated_flag_pcp
        ,  sum(case when network_flag_spec = 'In-Network' then 1 else 0 end) / count(*) as pct_in_network
           
    from   {TMP_DATABASE}.{PCP_REFS_TBL}
    where  specialty_type_pcp = 'PCP' 
    and    specialty_type_spec = 'Specialist'
    and    rend_pos_cat in ('ASC & HOPD', 'Hospital Inpatient') 
         
   group   by npi_pcp
       ,   specialty_cat_spec
       ,   affiliated_flag_pcp
   ) 
   select specialty_cat_spec
       ,  affiliated_flag_pcp
       ,  case when pct_in_network > 0.7 then 'Loyal'
               when pct_in_network > 0.3 then 'Splitter'
               else 'Dissenter' end as loyalty_flag_pcp
       ,  count(*) as count
   from   t1 
   group  by specialty_cat_spec
       ,  affiliated_flag_pcp
       ,  case when pct_in_network > 0.7 then 'Loyal'
               when pct_in_network > 0.3 then 'Splitter'
               else 'Dissenter' end
         
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{DATABASE}.page4_pcp_dist"

page4_pcp_dist = create_final_output_func(page4_pcp_dist_sdf)

insert_into_output_func(page4_pcp_dist.sort('specialty_cat_spec', 'affiliated_flag_pcp'), TBL_NAME)

# upload_to_s3_func(TBL_NAME)

# page4_pcp_dist.sort('specialty_cat_spec', 'affiliated_flag_pcp', 'loyalty_flag').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Top 10 PCPs; Patient Flow

# COMMAND ----------

# read in referrals and aggregate 

page4_patient_flow_pcps_sdf = spark.sql(f"""
    
    select npi_pcp
        ,  name_pcp
        ,  specialty_cat_spec
        ,  npi_spec
        ,  name_spec
        ,  network_flag_spec
        ,  count(*) as count
           
    from   {TMP_DATABASE}.{PCP_REFS_TBL}
    where  specialty_type_pcp = 'PCP' 
    and    specialty_type_spec = 'Specialist'
    and    rend_pos_cat in ('ASC & HOPD', 'Hospital Inpatient') 
    group by npi_pcp
        ,  name_pcp
        ,  specialty_cat_spec
        ,  npi_spec
        ,  name_spec
        ,  network_flag_spec

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{DATABASE}.page4_patient_flow_pcps"

page4_patient_flow_pcps = create_final_output_func(page4_patient_flow_pcps_sdf)

insert_into_output_func(page4_patient_flow_pcps.sort('npi_pcp', 'specialty_cat_spec'), TBL_NAME)

# upload_to_s3_func(TBL_NAME)

# page4_patient_flow_pcps.sort('npi_pcp', 'specialty_cat_spec').display()

# COMMAND ----------

# MAGIC %md ### 4. Top 10 Leakage Networks

# COMMAND ----------

# read in referrals and aggregate 

page4_net_leakage_sdf = spark.sql(f"""
    
    select specialty_cat_spec
        ,  network_id_spec as net_defhc_id_spec
        ,  ProfileName as net_defhc_name_spec
        ,  count(*) as count
    from   {TMP_DATABASE}.{PCP_REFS_TBL} a 
    join   MartDim.D_Profile b 
    on     a.network_id_spec = b.DefinitiveId
    where  network_id_pcp = {INPUT_NETWORK}
    and    network_id_spec != {INPUT_NETWORK}
    and    network_id_spec is not null 
    and    specialty_type_pcp = 'PCP' 
    and    specialty_type_spec = 'Specialist'
    and    rend_pos_cat in ('ASC & HOPD', 'Hospital Inpatient') 
    group  by specialty_cat_spec
        ,  network_id_spec
        ,  ProfileName

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{DATABASE}.page4_net_leakage"

page4_net_leakage = create_final_output_func(page4_net_leakage_sdf)

insert_into_output_func(page4_net_leakage.sort('specialty_cat_spec'), TBL_NAME)

# upload_to_s3_func(TBL_NAME)

# page4_net_leakage.sort('specialty_cat_spec').display()
