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
# MAGIC **NOTE**: DATABASE param below is value extracted from database widget, FAC_DATABASE is assigned in ProviderRunClass
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {FAC_DATABASE}.input_org_info
# MAGIC   - {FAC_DATABASE}.nearby_hcos_id
# MAGIC   - {FAC_DATABASE}.nearby_hcps
# MAGIC   - {FAC_DATABASE}.nearby_hcos_npi
# MAGIC   - {FAC_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {FAC_DATABASE}.{PCP_REFS_TBL}
# MAGIC   - {FAC_DATABASE}.inpat90_dashboard
# MAGIC   - MartDim.D_Organization
# MAGIC   - MxMart.F_MxClaim
# MAGIC   
# MAGIC **Outputs** (inserted into):
# MAGIC   - {DATABASE}.page1_toplevel_counts
# MAGIC   - {DATABASE}.page1_hosp_asc_pie
# MAGIC   - {DATABASE}.page1_hosp_asc_bar
# MAGIC   - {DATABASE}.page1_aff_spec_loyalty
# MAGIC   - {DATABASE}.page1_pcp_referrals
# MAGIC   - {DATABASE}.page1_vis90_inpat_stay

# COMMAND ----------

# MAGIC %run ./_funcs/_paths_include

# COMMAND ----------

import pyspark.sql.functions as F

from _funcs.setup_funcs import get_widgets, return_widget_values
from _funcs.ProviderRunClass import ProviderRun
from _funcs.chart_calc_funcs import get_top_values
from _funcs.params import HOSP_ASC_HOPD_SUBSET

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
    from  mxclaims_master_vw
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
    from   nearby_hcos_id_vw a 
    
    inner join  MartDim.D_Organization b 
    on     a.defhc_id = b.DefinitiveId 
    
    where  b.NPI in (select distinct FacilityNPI from MxMart.F_MxClaim where PlaceOfServiceCd = 21 
                     union distinct 
                     select distinct BillingProviderNPI from MxMart.F_MxClaim where PlaceOfServiceCd = 21) and
                   
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

cnt_firmtypes = spark.sql(f"""
    select count(distinct (case when FirmTypeName='Ambulatory Surgery Center' then defhc_id end)) as cnt_ascs
         , count(distinct (case when FirmTypeName='Physician Group' then defhc_id end)) as cnt_pgs
    
    from   nearby_hcos_id_vw
    where FirmTypeName in ('Ambulatory Surgery Center', 'Physician Group')

""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1Aiv. PCP and Specialist Count

# COMMAND ----------

# get a count of npi records by pcp_flag to get counts for spec vs pcp

cnt_provs = spark.sql(f"""
    select count(distinct (case when specialty_type='Specialist' then npi end)) as cnt_specialists
         , count(distinct (case when specialty_type='PCP' then npi end)) as cnt_pcps
    
    from   nearby_hcps_vw
    where specialty_type in ('Specialist', 'PCP')

""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1B. Combine All Counts, add Name and Output

# COMMAND ----------

# combine individual counts

all_counts = cnt_patient.join(cnt_inpat_hosp) \
                        .join(cnt_firmtypes) \
                        .join(cnt_provs) \
                        .withColumn('defhc_name', F.lit(ProvRunInstance.defhc_name))

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page1_toplevel_counts"

ProvRunInstance.create_final_output(all_counts, table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Hospital/ASC Shares

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2A. Pie chart Counts

# COMMAND ----------

hosp_asc_pie = get_top_values(intable = 'mxclaims_master_vw',
                               defhc = 'net_defhc',
                               defhc_value = ProvRunInstance.input_network,
                               max_row = 4,
                               strat_cols=['pos_cat'],
                               subset=f"where {HOSP_ASC_HOPD_SUBSET}") \
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

ProvRunInstance.create_final_output(hosp_asc_pie, table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2B. Bar chart Counts

# COMMAND ----------

hosp_asc_bar = get_top_values(intable = 'mxclaims_master_vw',
                               defhc = 'defhc',
                               defhc_value = DEFHC_ID,
                               max_row = 5,
                               strat_cols=['pos_cat'],
                               subset=f"where {HOSP_ASC_HOPD_SUBSET}") \
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

ProvRunInstance.create_final_output(page1_hosp_asc_bar, table=TBL_NAME)

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
           
    from mxclaims_master_vw
    where specialty_type = 'Specialist' and
          affiliation_2cat = 'Primary' and
          include_pie = 'Y'
          
    group by network_flag

    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page1_aff_spec_loyalty"

ProvRunInstance.create_final_output(aff_specs, table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. PCP Referrals Sent

# COMMAND ----------

# read in pcp referrals table and count total referrals by network_flag

pcp_referrals = spark.sql(f"""
    select network_flag_spec as network_flag
          ,count(*) as count
          
    from pcp_referrals_vw
          
    group by network_flag_spec
    order by network_flag_spec
    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page1_pcp_referrals"

ProvRunInstance.create_final_output(pcp_referrals, table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 5. Patient Visits 90 Days after Inpatient Stay 

# COMMAND ----------

# read in dashboard counts (already aggregated to level needed for chart)

pat_visits_sdf = spark.sql("""
    
    select place_of_service
           ,network_flag
           ,count
           
    from inpat90_dashboard_vw 
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page1_vis90_inpat_stay"

ProvRunInstance.create_final_output(pat_visits_sdf, table=TBL_NAME)

# COMMAND ----------

ProvRunInstance.exit_notebook()
