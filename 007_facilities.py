# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 007 Facilities
# MAGIC 
# MAGIC **Program:** 007_facilities
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider Facilities page <br>
# MAGIC <br>
# MAGIC **NOTE**: DATABASE and TMP_DATABASE params below are value extracted from database widget, value passed to TMP_DATABASE() lambda func param, tbl var names specified in params
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {TMP_DATABASE}.input_org_info
# MAGIC   - {TMP_DATABASE}.nearby_hcos_id
# MAGIC   - {TMP_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {TMP_DATABASE}.{PCP_REFS_TBL}
# MAGIC   
# MAGIC **Outputs** (inserted into):

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

import pandas as pd

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
# MAGIC ### 1. Map of Facilities

# COMMAND ----------

# subset nearby NPIs to non-null facility type and keep address info, look at count of facilities kept

facilities_sdf = spark.sql(f"""
        select distinct defhc_id as facility_id
                        ,defhc_name as facility_name
                        ,facility_type
                        ,latitude
                        ,longitude
                        
        from {TMP_DATABASE}.nearby_hcos_id
        where facility_type is not null and 
              primary=1
    """)

sdf_frequency(facilities_sdf, ['facility_type'], order='cols')

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table, load to s3

TBL_NAME = f"{DATABASE}.page5_facility_map"

page5_facility_map = create_final_output_func(facilities_sdf)

insert_into_output_func(page5_facility_map, TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Market Share

# COMMAND ----------

# to calculate market share (count of claims) by network for given facility type,
# call get_top_values() to count claims by facility type and identify top networks

market_pie = get_top_values(defhc = 'net_defhc',
                            defhc_value = INPUT_NETWORK,
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

page5_market_share = create_final_output_func(market_pie)

insert_into_output_func(page5_market_share.sort('facility_type', 'network_label'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

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

            from {TMP_DATABASE}.{MX_CLMS_TBL}
            where facility_type is not null
            group by facility_type 
                   , defhc_id
                   , defhc_name
                   , network_flag
            ) a

        """)

fac_ranked_sdf.sort('facility_type','rank').display()

# COMMAND ----------

# confirm distinct by defhc_id

sdf_check_distinct(fac_ranked_sdf, ['facility_id'])

# COMMAND ----------

# call create final output to join to base cols and add timestamp, filter to top 10, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page5_top10_fac"

page5_top10 = create_final_output_func(fac_ranked_sdf.filter(F.col('rank')<=10))

insert_into_output_func(page5_top10.sort('facility_type','rank'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. Top 10 PCPs

# COMMAND ----------

# for any given facility selected, will need to get top 10 PCPs referring TO that facility
# subset the referrals table to non-null spec facility_type (rendering facility), and count
# referrals by PCP info, facility and facility_type
# rank PCPs within given facility/facility type
# inner join to sdf with all nearby IDs (referrals are only filtered to nearby HCPs, not nearby HCOs)

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
               ,network_flag_spec as network_flag
               ,count(*) as count

        from {TMP_DATABASE}.{PCP_REFS_TBL}
        where facility_type_spec is not null

        group by facility_type_spec
                ,npi_pcp
                ,name_pcp
                ,npi_url_pcp
                ,defhc_id_spec
                ,network_flag_spec
        ) a

""")

pcp_ranked_sdf.filter(F.col('rank')<=10).count()

# COMMAND ----------

# filter to IDs in nearby facilities and compare count with ALL facilities

pcp_top10_sdf = pcp_ranked_sdf.filter(F.col('rank')<=10) \
                              .join(facilities_sdf.select('facility_id'), \
                                   ['facility_id'], \
                                   'inner')

pcp_top10_sdf.count()

# COMMAND ----------

# get count of unique facilities by type retained (compare to map counts)

sdf_frequency(pcp_top10_sdf.select('facility_id','facility_type').distinct(), ['facility_type'], order='cols')

# COMMAND ----------

# call create final output to join to base cols and add timestamp, filter to top 10, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page5_top10_pcp"

page5_top10_pcp = create_final_output_func(pcp_top10_sdf)

insert_into_output_func(page5_top10_pcp.sort('facility_type','facility_id', 'rank'), TBL_NAME)

upload_to_s3_func(TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 5. Top 10 Facilities Post-Discharge

# COMMAND ----------

# until we decide how to calculate inpatient stays, will just create dummy records for this,
# with 10 facilities and counts for each Hospital in facilities_sdf

numbers = list(range(1, 11))

fac_samp_dict = {
    'discharge_facility_id': numbers,
    'discharge_facility_name': [f"Dummy Facility {x}" for x in numbers],
    'count': [x*10 for x in numbers[::-1]],
    'rank': numbers
}

dummy_sdf = spark.createDataFrame(pd.DataFrame(fac_samp_dict))

dummy_sdf.display()

# COMMAND ----------

top10_postdis_sdf = facilities_sdf.filter(F.col('facility_type')=='Hospital') \
                                  .select('facility_id') \
                                  .join(dummy_sdf)

top10_postdis_sdf.sort('facility_id','rank').display()

# COMMAND ----------

# call create final output to join to base cols and add timestamp, filter to top 10, insert output for insert into table, and load to s3

TBL_NAME = f"{DATABASE}.page5_top10_postdis"

top10_postdis = create_final_output_func(top10_postdis_sdf)

insert_into_output_func(top10_postdis.sort('facility_id', 'rank'), TBL_NAME)

upload_to_s3_func(TBL_NAME)
