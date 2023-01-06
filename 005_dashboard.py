# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 005 Dashboard
# MAGIC 
# MAGIC **Program:** 005_dashboard
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
# MAGIC   - {TMP_DATABASE}.{AFF_CLMS_TBL}
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

INPUT_NETWORK = hive_to_df(f"{TMP_DATABASE}.input_org_info").select('input_network').collect()[0][0]

# COMMAND ----------

# create base df to create partial for create_final_output function

base_sdf = base_output_table(DEFHC_ID, RADIUS, START_DATE, END_DATE)
create_final_output_func = partial(create_final_output, base_sdf)

# create partial for insert_into_output function

insert_into_output_func = partial(insert_into_output, DEFHC_ID, RADIUS, START_DATE, END_DATE)

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

# get count of distinct facilites by firm type and facility name

hco_summary = spark.sql(f"""
    select FirmTypeName
         , FacilityTypeName
         , count(distinct defhc_id) as providers
    
    from   {TMP_DATABASE}.nearby_hcos_id
    
    group by FirmTypeName
            , FacilityTypeName
""")

# COMMAND ----------

# sum total providers by firm type

firms_summary = hco_summary.groupBy('FirmTypeName') \
                           .agg(F.sum(F.col('providers')).alias('cnt_providers')) \
                           .sort('FirmTypeName')

firms_summary.display()

# COMMAND ----------

# extract asc and PG counts (separately)

cnt_asc = firms_summary.filter(F.col('FirmTypeName')=='Ambulatory Surgery Center').withColumnRenamed('cnt_providers', 'cnt_ascs').select('cnt_ascs')

cnt_pg = firms_summary.filter(F.col('FirmTypeName')=='Physician Group').withColumnRenamed('cnt_providers', 'cnt_pgs').select('cnt_pgs')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1Aiv. PCP and Specialist Count

# COMMAND ----------

# get a count of npi records by pcp_flag to get counts for spec vs pcp

nearby_hcps = hive_to_df(f"{TMP_DATABASE}.nearby_hcps")

pcp_spec_summary = nearby_hcps.groupby('pcp_flag') \
                              .agg(F.count(F.col('npi')).alias('cnt_providers')) \
                              .sort('pcp_flag')

pcp_spec_summary.display()

# COMMAND ----------

# extract pcp and spec counts (separately)

cnt_spec = pcp_spec_summary.filter(F.col('pcp_flag')==0).withColumnRenamed('cnt_providers', 'cnt_specialists').select('cnt_specialists')

cnt_pcp = pcp_spec_summary.filter(F.col('pcp_flag')==1).withColumnRenamed('cnt_providers', 'cnt_pcps').select('cnt_pcps')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1B. Combine and Output All Counts

# COMMAND ----------

# combine individual counts

all_counts = cnt_patient.join(cnt_inpat_hosp) \
                        .join(cnt_pg) \
                        .join(cnt_asc) \
                        .join(cnt_pcp) \
                        .join(cnt_spec)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

page1_toplevel_counts = create_final_output_func(all_counts)

insert_into_output_func(page1_toplevel_counts, f"{DATABASE}.page1_toplevel_counts")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Hospital/ASC Shares

# COMMAND ----------

def hosp_asc_counts(defhc, defhc_value, max_row):
    """
    Function hosp_asc_counts() to get aggregate claim counts for either pie chart (by network) or bar chart (by facility)
        will subset claims to hospital and ASC pos, and get count of claims by pos_cat and defhc_id or net_defhc_id
        will create collapsed/formatted names and labels to collapse >4th or >5th positions (pie or bar, respectively)
        
    params:
        defhc str: prefix to id and name cols to specify network or facility, = 'net_defhc' for network, 'defhc' for facility
        defhc_value int: value for either your network or your facility
        max_row int: value for max row to keep, all others are collapsed (4 or 5)
        
    returns:
        spark df with claim counts at network or facility level
    
    """
    
    # assign place_name based on defhc (whether specified as defhc or net_defhc)
    
    if defhc == 'defhc':
        place_name = 'Facility'
        
    elif defhc == 'net_defhc':
        place_name = 'Network'
    
    return spark.sql(f"""

        select *
               
               -- create collapsed/formatted values based on max row number to collapse, and label formatted specifically for your network/facility
            
               ,case when rn < {max_row} then {defhc}_id 
                     else Null
                     end as {defhc}_id_collapsed

               ,case when rn < {max_row} then {defhc}_name
                     else 'Other' 
                     end as {place_name.lower()}_name

              ,case when {defhc}_id = {defhc_value} then "1. Your {place_name}"
                    when rn < {max_row} then concat(rn + 1, ". {place_name} ", rn + 1)
                    else "{max_row+1}. Other"
                    end as {place_name.lower()}_label

        from (

            select *
            
                   -- create rn to order networks separately for hospital/ASC, with your network as 0, null IDs as 100 (ignore), and all others by claim counts
                   
                   ,case when {defhc}_id = {defhc_value} then 0 
                         when {defhc}_id is null then 100
                         else row_number() over (partition by place_of_service
                                                 order by case when {defhc}_id is null or {defhc}_id = {defhc_value} then 0 
                                                               else cnt_claims 
                                                               end desc) 
                          end as rn

                  ,case when {defhc}_id = {defhc_value} then concat('*', {defhc}_name_raw, '*') 
                        else {defhc}_name_raw
                        end as {defhc}_name
           from (
                select {defhc}_id
                       , {defhc}_name as {defhc}_name_raw
                       , pos_cat as place_of_service
                       , count(*) as cnt_claims

                from {TMP_DATABASE}.{MX_CLMS_TBL}
                where pos_cat in ('ASC & HOPD', 'Hospital Inpatient')
                group by {defhc}_id
                       , {defhc}_name
                       , pos_cat
                ) a
            ) b
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2A. Pie chart Counts

# COMMAND ----------

hosp_asc_pie = hosp_asc_counts(defhc = 'net_defhc',
                               defhc_value = INPUT_NETWORK,
                               max_row = 4)

hosp_asc_pie.createOrReplaceTempView('hosp_asc_pie_vw')

# COMMAND ----------

# display top 10 per pos category

hosp_asc_pie.filter(F.col('rn')<10).sort('place_of_service', 'rn').display()

# COMMAND ----------

# to get patient counts for pie chart, must join individual patients back to the above view to re-count unique patients for the collapsed 'Other' group
# if there are 0 counts in the other group (<5 total networks for the given POS) delete the record(s)

hosp_asc_pie = spark.sql(f"""

    select  place_of_service
          , network_label
          , network_name
          , count(*) as count
          
   from (

        select a.*
              , b.patientid

        from hosp_asc_pie_vw a
             left join
             {TMP_DATABASE}.{MX_CLMS_TBL} b

       on a.net_defhc_id = b.net_defhc_id and 
          a.place_of_service = b.pos_cat
          
      )  c
      
  group by place_of_service
          , network_label
          , network_name
          
  having (count > 0) or (network_name != 'Other')
          
     
    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

page1_hosp_asc_pie = create_final_output_func(hosp_asc_pie)

insert_into_output_func(page1_hosp_asc_pie.sort('place_of_service', 'network_label'), f"{DATABASE}.page1_hosp_asc_pie")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2B. Bar chart Counts

# COMMAND ----------

hosp_asc_bar = hosp_asc_counts(defhc = 'defhc',
                               defhc_value = DEFHC_ID,
                               max_row = 5)

# COMMAND ----------

# display top 10 per pos category

hosp_asc_bar.filter(F.col('rn')<10).sort('place_of_service', 'rn').display()

# COMMAND ----------

# keep needed cols, drop rows in >5th position for output, and
# call create final counts to join to base cols and add timestamp, and insert output for insert into table

page1_hosp_asc_bar = hosp_asc_bar.filter(F.col('defhc_id_collapsed').isNotNull()) \
                                  .select('place_of_service', 'facility_label', 'facility_name', 'cnt_claims') \
                                  .withColumnRenamed('cnt_claims', 'count')

page1_hosp_asc_bar = create_final_output_func(page1_hosp_asc_bar)

insert_into_output_func(page1_hosp_asc_bar.sort('place_of_service', 'facility_label'), f"{DATABASE}.page1_hosp_asc_bar")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Affiliated Specialists Performing Loyalty

# COMMAND ----------

# read in affiliated provider claims, subset to in/out-patient hospital and specialist provider
# count number of unique patients by network_flag

aff_specs = spark.sql(f"""

    select network_flag
           ,count(*) as count
           
    from {TMP_DATABASE}.{AFF_CLMS_TBL}
    where pcp_flag = 0 and
          pos_cat in ('Hospital Inpatient', 'ASC & HOPD')
          
    group by network_flag

    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

page1_aff_spec_loyalty = create_final_output_func(aff_specs)

insert_into_output_func(page1_aff_spec_loyalty.sort('network_flag'), f"{DATABASE}.page1_aff_spec_loyalty")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. PCP Referrals Sent

# COMMAND ----------

# read in pcp referrals table and count total referrals by network_flag

pcp_referrals = spark.sql(f"""
    select network_flag
          ,count(*) as count
          
    from {TMP_DATABASE}.{PCP_REFS_TBL}
    group by network_flag
    order by network_flag
    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

page1_pcp_referrals = create_final_output_func(pcp_referrals)

insert_into_output_func(page1_pcp_referrals.sort('network_flag'), f"{DATABASE}.page1_pcp_referrals")

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

# call create final output to join to base cols and add timestamp, and insert output for insert into table

page1_vis90_inpat_stay = create_final_output_func(patient_visits_after_inpatient)

insert_into_output_func(page1_vis90_inpat_stay.sort('network_flag', 'place_of_service'), f"{DATABASE}.page1_vis90_inpat_stay")
