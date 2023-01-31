# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 002 Dashboard
# MAGIC 
# MAGIC **Program:** 002_dashboard
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider dashboard <br>
# MAGIC <br>
# MAGIC **NOTE**: DATABASE and TMP_DATABASE params below are value extracted from database widget, value passed to TMP_DATABASE() lambda func param, tbl var names specified in params
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {TMP_DATABASE}.input_org_info
# MAGIC   - {TMP_DATABASE}.nearby_hcos_id
# MAGIC   - {TMP_DATABASE}.nearby_hcps
# MAGIC   - {TMP_DATABASE}.nearby_hcos_npi
# MAGIC   - {TMP_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {TMP_DATABASE}.{PCP_REFS_TBL}
# MAGIC   - MartDim.D_Organization
# MAGIC   - MxMart.F_MxClaim_v2
# MAGIC   
# MAGIC **Outputs** (inserted into):
# MAGIC   - {DATABASE}.page1_toplevel_counts
# MAGIC   - {DATABASE}.page1_hosp_asc_pie
# MAGIC   - {DATABASE}.page1_hosp_asc_bar
# MAGIC   - {DATABASE}.page1_aff_spec_loyalty
# MAGIC   - {DATABASE}.page1_pcp_referrals
# MAGIC   - {DATABASE}.page1_vis90_inpat_stay

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F

from functools import partial

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

# setup: 
#  create/get widget values, assign temp database and network

RUN_VALUES = get_widgets()

DEFHC_ID, RADIUS, START_DATE, END_DATE, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'DATABASE', 'RUN_QC'])

TMP_DATABASE = GET_TMP_DATABASE(DATABASE)

INPUT_NETWORK, DEFHC_NAME = sdf_return_row_values(hive_to_df(f"{TMP_DATABASE}.input_org_info"), ['input_network', 'defhc_name'])

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
# MAGIC ### 1. Summary Counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1A. Get Individual Counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1Ai. Patient Count

# COMMAND ----------

# get count of total unique patients from claims

cnt_patient = spark.sql(f"""

    select count(distinct PatientId) as cnt_patients
    from  {TMP_DATABASE}.{MX_CLMS_TBL}
""")

cnt_patient.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1Aii. Inpatient Hospital Count

# COMMAND ----------

# get count of unique inpatient hospitals by joining to d_organization to identify active NPIs,
# and requiring the NPI have at least one claim (all time) as facility or billing NPI with POS = 21
# subset firm type to hospital

cnt_inpat_hosp = spark.sql(f"""

    select count(distinct a.defhc_id) as cnt_ip_hospitals
    from   {TMP_DATABASE}.nearby_hcos_id a 
    
    inner join  MartDim.D_Organization b 
    on     a.defhc_id = b.DefinitiveId 
    
    where  b.NPI in (select distinct FacilityNPI from MxMart.F_MxClaim_v2 where PlaceOfServiceCd = 21 
                     union distinct 
                     select distinct BillingProviderNPI from MxMart.F_MxClaim_v2 where PlaceOfServiceCd = 21) and
                   
           b.ActiveRecordInd = 'Y' and
           a.FirmTypeName = 'Hospital'
""")

cnt_inpat_hosp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1Aiii. ASC and PG Count

# COMMAND ----------

# get count of distinct facilites by firm type

hco_summary = spark.sql(f"""
    select FirmTypeName
         , count(distinct defhc_id) as cnt_providers
    
    from   {TMP_DATABASE}.nearby_hcos_id
    where FirmTypeName in ('Ambulatory Surgery Center', 'Physician Group')
    
    group by FirmTypeName
    order by FirmTypeName
""")

hco_summary.display()

# COMMAND ----------

# extract asc and PG counts (separately)

cnt_asc = hco_summary.filter(F.col('FirmTypeName')=='Ambulatory Surgery Center').withColumnRenamed('cnt_providers', 'cnt_ascs').select('cnt_ascs')

cnt_pg = hco_summary.filter(F.col('FirmTypeName')=='Physician Group').withColumnRenamed('cnt_providers', 'cnt_pgs').select('cnt_pgs')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1Aiv. PCP and Specialist Count

# COMMAND ----------

# get a count of npi records by pcp_flag to get counts for spec vs pcp

nearby_hcps = hive_to_df(f"{TMP_DATABASE}.nearby_hcps")

pcp_spec_summary = nearby_hcps.groupby('specialty_type') \
                              .agg(F.count(F.col('npi')).alias('cnt_providers')) \
                              .sort('specialty_type')

pcp_spec_summary.display()

# COMMAND ----------

# extract pcp and spec counts (separately)

cnt_spec = pcp_spec_summary.filter(F.col('specialty_type')=='Specialist').withColumnRenamed('cnt_providers', 'cnt_specialists').select('cnt_specialists')

cnt_pcp = pcp_spec_summary.filter(F.col('specialty_type')=='PCP').withColumnRenamed('cnt_providers', 'cnt_pcps').select('cnt_pcps')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1B. Combine All Counts, add Name and Output

# COMMAND ----------

# combine individual counts

all_counts = cnt_patient.join(cnt_inpat_hosp) \
                        .join(cnt_pg) \
                        .join(cnt_asc) \
                        .join(cnt_pcp) \
                        .join(cnt_spec) \
                        .withColumn('defhc_name', F.lit(DEFHC_NAME))

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page1_toplevel_counts"

page1_toplevel_counts = create_final_output_func(all_counts)

insert_into_output_func(page1_toplevel_counts, TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Hospital/ASC Shares

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2A. Pie chart Counts

# COMMAND ----------

hosp_asc_pie = get_top_values(intable = f"{TMP_DATABASE}.{MX_CLMS_TBL}",
                               defhc = 'net_defhc',
                               defhc_value = INPUT_NETWORK,
                               max_row = 4,
                               strat_cols=['pos_cat'],
                               subset="where (pos_cat='ASC & HOPD' and facility_type in ('Ambulatory Surgical Center', 'Hospital')) or (pos_cat='Hospital Inpatient' and facility_type='Hospital')") \
              .withColumnRenamed('pos_cat', 'place_of_service')

hosp_asc_pie.createOrReplaceTempView('hosp_asc_pie_vw')

# COMMAND ----------

# display top 10 per pos category

hosp_asc_pie.filter(F.col('rn')<10).sort('place_of_service', 'rn').display()

# COMMAND ----------

# collapse all the networks in the "other" category
# if there are 0 counts in the other group (<5 total networks for the given POS) delete the record(s)

hosp_asc_pie = spark.sql(f"""

    select  place_of_service
          , network_label
          , network_name
          , sum(cnt_claims) as count
          
    from hosp_asc_pie_vw
    
    group by place_of_service
            , network_label
            , network_name
          
     having (count > 0) or (network_name != 'Other')     
     
    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page1_hosp_asc_pie"

page1_hosp_asc_pie = create_final_output_func(hosp_asc_pie)

insert_into_output_func(page1_hosp_asc_pie.sort('place_of_service', 'network_label'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2B. Bar chart Counts

# COMMAND ----------

hosp_asc_bar = get_top_values(intable = f"{TMP_DATABASE}.{MX_CLMS_TBL}",
                               defhc = 'defhc',
                               defhc_value = DEFHC_ID,
                               max_row = 5,
                               strat_cols=['pos_cat'],
                               subset="where (pos_cat='ASC & HOPD' and facility_type in ('Ambulatory Surgical Center', 'Hospital')) or (pos_cat='Hospital Inpatient' and facility_type='Hospital')") \
              .withColumnRenamed('pos_cat', 'place_of_service')

# COMMAND ----------

# display top 10 per pos category

hosp_asc_bar.filter(F.col('rn')<10).sort('place_of_service', 'rn').display()

# COMMAND ----------

# keep needed cols, drop rows in >5th position for output, and
# call create final counts to join to base cols and add timestamp, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page1_hosp_asc_bar"

page1_hosp_asc_bar = hosp_asc_bar.filter(F.col('defhc_id_collapsed').isNotNull()) \
                                  .select('place_of_service', 'facility_label', 'facility_name', 'cnt_claims') \
                                  .withColumnRenamed('cnt_claims', 'count')

page1_hosp_asc_bar = create_final_output_func(page1_hosp_asc_bar)

insert_into_output_func(page1_hosp_asc_bar.sort('place_of_service', 'facility_label'), f"{DATABASE}.page1_hosp_asc_bar")

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Affiliated Specialists Performing Loyalty

# COMMAND ----------

# read in claim subset to affiliated specialists to include in pie chart, specialist provider
# count number of unique patients by network_flag

aff_specs = spark.sql(f"""

    select network_flag
           ,count(*) as count
           
    from {TMP_DATABASE}.{MX_CLMS_TBL}
    where specialty_type = 'Specialist' and
          affiliated_flag = 'Affiliated' and
          include_pie = 'Y'
          
    group by network_flag

    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page1_aff_spec_loyalty"

page1_aff_spec_loyalty = create_final_output_func(aff_specs)

insert_into_output_func(page1_aff_spec_loyalty.sort('network_flag'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. PCP Referrals Sent

# COMMAND ----------

# read in pcp referrals table and count total referrals by network_flag
# subset to nearby providers and valid specialist facility

pcp_referrals = spark.sql(f"""
    select network_flag_spec as network_flag
          ,count(*) as count
          
    from {TMP_DATABASE}.{PCP_REFS_TBL}
    
    where nearby_pcp=1 and 
          nearby_spec=1 and
          network_flag_spec is not null
          
    group by network_flag_spec
    order by network_flag_spec
    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page1_pcp_referrals"

page1_pcp_referrals = create_final_output_func(pcp_referrals)

insert_into_output_func(page1_pcp_referrals.sort('network_flag'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 5. Patient Visits 90 Days after Inpatient Stay 

# COMMAND ----------


# TODO: fix the below, just using this original query to get tables populated with some counts 

patient_visits_after_inpatient = spark.sql(f"""
with t1 as (
SELECT DISTINCT patientid,
       mxclaimdatekey
  FROM {TMP_DATABASE}.{MX_CLMS_TBL}
 WHERE PlaceOfServiceCd = 21
 AND defhc_id = {DEFHC_ID}
 ),
 t2 as (
 SELECT a.patientid,
        coalesce(a.pos_cat, 'Other') as place_of_service,
        a.network_flag
  FROM  {TMP_DATABASE}.{MX_CLMS_TBL} a
  JOIN T1 
  ON t1.patientid = a.patientid 
WHERE a.mxclaimdatekey <= date_add(t1.mxclaimdatekey, 90)
AND a.mxclaimdatekey > t1.mxclaimdatekey
)
SELECT network_flag
       ,place_of_service
       ,count(*) as count 
 FROM t2 
 GROUP BY 1,2""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page1_vis90_inpat_stay"

page1_vis90_inpat_stay = create_final_output_func(patient_visits_after_inpatient)

insert_into_output_func(page1_vis90_inpat_stay.sort('network_flag', 'place_of_service'), TBL_NAME)

upload_to_s3_func(TBL_NAME)
