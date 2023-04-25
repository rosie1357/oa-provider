# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 006 Facilities
# MAGIC 
# MAGIC **Program:** 006_facilities
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider Facilities page <br>
# MAGIC <br>
# MAGIC **NOTE**: DATABASE param below is value extracted from database widget, FAC_DATABASE is assigned in ProviderRunClass
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {FAC_DATABASE}.input_org_info
# MAGIC   - {FAC_DATABASE}.nearby_hcos_id
# MAGIC   - {FAC_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {FAC_DATABASE}.{PCP_REFS_TBL}
# MAGIC   - {FAC_DATABASE}.inpat90_facilities
# MAGIC   
# MAGIC **Outputs** (inserted into):
# MAGIC   - {DATABASE}.page5_facility_map
# MAGIC   - {DATABASE}.page5_market_share
# MAGIC   - {DATABASE}.page5_top10_fac
# MAGIC   - {DATABASE}.page5_top10_pcp
# MAGIC   - {DATABASE}.page5_top10_postdis

# COMMAND ----------

# MAGIC %run ./_funcs/_paths_include

# COMMAND ----------

from _funcs.setup_funcs import get_widgets, return_widget_values
from _funcs.ProviderRunClass import ProviderRun
from _funcs.chart_calc_funcs import get_top_values

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
# MAGIC ### 1. Map of Facilities

# COMMAND ----------

# subset nearby NPIs to non-null facility type, primary locations only

facilities_sdf = spark.sql(f"""
        select distinct defhc_id as facility_id
                        ,defhc_name as facility_name
                        ,facility_type
                        ,latitude
                        ,longitude
                        
        from nearby_hcos_id_vw
        where facility_type is not null and 
              primary=1
    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page5_facility_map"

ProvRunInstance.create_final_output(facilities_sdf, table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Market Share

# COMMAND ----------

# to calculate market share (count of claims) by network for given facility type,
# call get_top_values() to count claims by facility type and identify top networks

market_pie = get_top_values(intable = 'mxclaims_master_vw',
                            defhc = 'net_defhc',
                            defhc_value = ProvRunInstance.input_network,
                            max_row = 4,
                            strat_cols=['facility_type'],
                            subset="where facility_type is not null"
                           )

market_pie.createOrReplaceTempView('market_pie_vw')

# COMMAND ----------

# collapse all the networks in the "other" category
# if there are 0 counts in the other group (<5 total networks for the given POS) delete the record(s)

market_pie = spark.sql(f"""

    select  facility_type
          , network_label
          , network_name
          , sum(cnt_claims) as count
          
    from market_pie_vw
    
    group by facility_type
            , network_label
            , network_name
          
     having (count > 0) or (network_name != 'Other')     
     
    """)

# COMMAND ----------

# call create final output to join to base cols and add timestamp, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page5_market_share"

ProvRunInstance.create_final_output(market_pie, table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Top 10 Facilities

# COMMAND ----------

# to calculate top 10 facilities by count of claims for given facility type,
# read in full claims and order by claim count by facility name

fac_ranked_sdf = spark.sql(f"""

        select *
                , row_number() over (partition by facility_type 
                                     order by count desc)
                               as rank
       from (
            select facility_type 
                   , defhc_id as facility_id
                   , defhc_name as facility_name
                   , network_flag
                   , count(*) as count

            from mxclaims_master_vw
            where facility_type is not null
            group by facility_type 
                   , defhc_id
                   , defhc_name
                   , network_flag
            ) a

        """)

fac_ranked_sdf.sort('facility_type','rank').display()

# COMMAND ----------

# confirm distinct by facility_id

ProvRunInstance.test_distinct(sdf = fac_ranked_sdf,
                              name = 'Facilities for Top 10 Chart',
                              cols = ['facility_id'],
                              to_subset = False
                              )

# COMMAND ----------

# call create final output to join to base cols and add timestamp, filter to top 10, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page5_top10_fac"

ProvRunInstance.create_final_output(fac_ranked_sdf.filter(F.col('rank')<=10), table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. Top 10 PCPs

# COMMAND ----------

# for any given facility selected, will need to get top 10 PCPs referring TO that facility
# subset the referrals table to non-null spec facility_type (rendering facility), and count
# referrals by PCP info, facility and facility_type
# rank PCPs within given facility/facility type

pcp_ranked_sdf = spark.sql(f"""

    select *
           , row_number() over (partition by facility_type, facility_id
                                order by count desc)
                          as rank
    
    from (

        select facility_type_spec as facility_type
               ,npi_pcp
               ,name_pcp
               ,npi_url_pcp
               ,defhc_id_spec as facility_id
               ,count(*) as count

        from pcp_referrals_vw
        where facility_type_spec is not null

        group by facility_type_spec
                ,npi_pcp
                ,name_pcp
                ,npi_url_pcp
                ,defhc_id_spec
        ) a

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, filter to top 10, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page5_top10_pcp"

ProvRunInstance.create_final_output(pcp_ranked_sdf.filter(F.col('rank')<=10), table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 5. Top 10 Facilities Post-Discharge

# COMMAND ----------

# read in discharge location counts, subset to top 10 per ID

fac_discharge_sdf = spark.sql("""
    
    select facility_id
           ,defhc_id as discharge_facility_id
           ,defhc_name as discharge_facility_name
           ,defhc_fac_type as discharge_facility_type
           ,count
           ,row_number() over (partition by facility_id
                                order by count desc)
                          as rank
    
    from inpat90_facilities_vw
    where defhc_id is not null
    
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, filter to top 10, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page5_top10_postdis"

ProvRunInstance.create_final_output(fac_discharge_sdf.filter(F.col('rank')<=10), table=TBL_NAME)

# COMMAND ----------

ProvRunInstance.exit_notebook(final_nb=True)
