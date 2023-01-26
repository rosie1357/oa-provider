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
        select distinct defhc_id
                        ,defhc_name
                        ,facility_type
                        ,latitude
                        ,longitude
                        
        from {TMP_DATABASE}.nearby_hcos_id
        where facility_type is not null and 
              primary=1
    """)

sdf_frequency(facilities_sdf, ['facility_type'])

# COMMAND ----------



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
                            strat_col='facility_type',
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
