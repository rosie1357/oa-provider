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
# MAGIC **NOTE**: DATABASE param below is value extracted from database widget, FAC_DATABASE is assigned in ProviderRunClass
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

# create all widgets
 
RUN_VALUES = get_widgets()

# COMMAND ----------

# get widget values and use to create instance of provider run class

DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'SUBSET_LT18', 'DATABASE', 'RUN_QC'])

ProvRunInstance = ProviderRun(DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, DATABASE, RUN_QC)

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

ProvRunInstance.create_final_output(page4_loyalty_map_sdf, table=TBL_NAME)

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

ProvRunInstance.create_final_output(page4_pcp_dist_sdf, table=TBL_NAME)

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

ProvRunInstance.create_final_output(page4_patient_flow_pcps_sdf, table=TBL_NAME)

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
    where  net_defhc_id_pcp = {ProvRunInstance.input_network}
    and    net_defhc_id_spec != {ProvRunInstance.input_network}
    and    net_defhc_id_spec is not null
    
    group  by specialty_cat_spec
        ,  net_defhc_id_spec
        ,  net_defhc_name_spec

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{DATABASE}.page4_net_leakage"

ProvRunInstance.create_final_output(page4_net_leakage_sdf, table=TBL_NAME)

# COMMAND ----------

ProvRunInstance.exit_notebook()
