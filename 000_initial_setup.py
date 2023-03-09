# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: Initial (One-Time) Setup
# MAGIC 
# MAGIC **Program:** 000_initial_setup
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to do initial setup (creation of ref tables, etc) for provider dashboard <br>
# MAGIC <br>
# MAGIC 
# MAGIC **NOTE**: DATABASE param below is value extracted from database widget
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - /dbfs/FileStore/datascience/oa_provider/Appendix_1__Provider_OA___Specialist_vs_PCP_Assignment.xlsx
# MAGIC 
# MAGIC **Outputs** (Lookup tables):
# MAGIC   - {DATABASE}.hcp_specialty_assignment
# MAGIC   - {DATABASE}.pos_category_assign
# MAGIC   
# MAGIC **Outputs** (Empty tables for measure inserts):
# MAGIC   - {DATABASE}.page1_toplevel_counts
# MAGIC   - {DATABASE}.page1_hosp_asc_pie
# MAGIC   - {DATABASE}.page1_hosp_asc_bar
# MAGIC   - {DATABASE}.page1_aff_spec_loyalty
# MAGIC   - {DATABASE}.page1_pcp_referrals
# MAGIC   - {DATABASE}.page1_vis90_inpat_stay

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

import pandas as pd

from functools import reduce
from pyspark.sql.types import StringType, IntegerType, BooleanType

# COMMAND ----------

RUN_VALUES = get_widgets(include_widgets=[5])

DATABASE = return_widget_values(RUN_VALUES, ['DATABASE'])[0]

# COMMAND ----------

# list of lookup tables to create

LOOKUP_TABLES = [f"{DATABASE}.hcp_specialty_assignment",
                 f"{DATABASE}.pos_category_assign"]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Create pcp vs specialist lookup tables

# COMMAND ----------

# import lookup table, rename cols to save in hive

spec_df = pd.read_excel("/dbfs/FileStore/datascience/oa_provider/Appendix_1__Provider_OA___Specialist_vs_PCP_Assignment.xlsx")

spec_df.columns = ['specialty_id', 'specialty_name', 'specialty_cat', 'specialty_type', 'include_pie']
spec_df.head()

# COMMAND ----------

# save to output table in database

pyspark_to_hive(spark.createDataFrame(spec_df),
               LOOKUP_TABLES[0])

hive_sample(LOOKUP_TABLES[0])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Create POS category table

# COMMAND ----------

# dictionary of currently assigned values

POS_CATS = {'Hospital Inpatient': [21],
            'ASC & HOPD': [19, 22, 24],
            'Office': [11],
            'Post-Acute': [31, 32, 33, 34, 61, 62, 12, 13]
           }

# COMMAND ----------

# create one dataframe per category and then concat all

dfs = []
for name, values in POS_CATS.items():
    dfs += [pd.DataFrame(values, columns = ['PlaceOfServiceCd']).assign(pos_cat=name)]

pos_lookups = reduce(lambda x, y: pd.concat([x, y]), dfs).reset_index(drop=True)
pos_lookups['PlaceOfServiceCd'] = pos_lookups['PlaceOfServiceCd'].apply(lambda x: str(x).zfill(2))

pos_lookups.head(50)

# COMMAND ----------

# save as output table

pyspark_to_hive(spark.createDataFrame(pos_lookups),
               LOOKUP_TABLES[1])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Create empty measure tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### A. Record count/final run tables

# COMMAND ----------

schema = create_empty_output({'database': StringType(),
                              'table_name': StringType(),
                              'count': IntegerType(),
                              'run_number': IntegerType(),
                              'most_recent': BooleanType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.record_counts")

# COMMAND ----------

schema = create_empty_output({'success': BooleanType(),
                              'run_number': IntegerType(),
                              'most_recent': BooleanType(),
                              'fail_reason': StringType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.run_status")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### B. Dashboard (Page 1)

# COMMAND ----------

# page1_toplevel_counts

schema = create_empty_output({'defhc_name': StringType(),
                              'cnt_patients': IntegerType(),
                              'cnt_ip_hospitals':  IntegerType(),
                              'cnt_pgs': IntegerType(),
                              'cnt_ascs': IntegerType(),
                              'cnt_pcps': IntegerType(),
                              'cnt_specialists': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page1_toplevel_counts")

# COMMAND ----------

# page1_hosp_asc_pie

schema = create_empty_output({'place_of_service': StringType(),
                              'network_label':  StringType(),
                              'network_name': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page1_hosp_asc_pie")

# COMMAND ----------

# page1_hosp_asc_bar

schema = create_empty_output({'place_of_service': StringType(),
                              'facility_label':  StringType(),
                              'facility_name': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page1_hosp_asc_bar")

# COMMAND ----------

# page1_aff_spec_loyalty

schema = create_empty_output({'network_flag': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page1_aff_spec_loyalty")

# COMMAND ----------

# page1_pcp_referrals

schema = create_empty_output({'network_flag': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page1_pcp_referrals")

# COMMAND ----------

# page1_vis90_inpat_stay

schema = create_empty_output({'network_flag': StringType(),
                              'place_of_service': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page1_vis90_inpat_stay")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### C. Patients (Page 2)

# COMMAND ----------

# placeholder

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### D. Specialists (Page 3)

# COMMAND ----------

# page3_top_panel_specialists

schema = create_empty_output({'npi': IntegerType(),
                              'name': StringType(),
                              'npi_url': StringType(),
                              'specialty_cat': StringType(),
                              'affiliation_2cat': StringType(),
                              'count_in_network': IntegerType(),
                              'count_out_of_network': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page3_top_panel_specialists")

# COMMAND ----------

# page3_shares

schema = create_empty_output({'net_defhc_id': IntegerType(),
                              'net_defhc_name': StringType(),
                              'specialty_cat': StringType(),
                              'affiliation_2cat': StringType(),
                              'place_of_service': StringType(),
                              'network_flag': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page3_shares", overwrite_schema='true')

# COMMAND ----------

# page3_top_pcp_flow

schema = create_empty_output({'npi_pcp': IntegerType(),
                              'name_pcp': StringType(),
                              'npi_url_pcp': StringType(),
                              'npi_spec': IntegerType(),
                              'name_spec': StringType(),
                              'npi_url_spec': StringType(),
                              'specialty_cat_spec': StringType(),
                              'affiliation_spec': StringType(),
                              'affiliation_2cat_spec': StringType(),
                              'network_flag_spec': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page3_top_pcp_flow")

# COMMAND ----------

# MAGIC %md #### E. PCPs (Page 4)

# COMMAND ----------

# page4_loyalty_map_pcps

schema = create_empty_output({'specialty_cat_spec': StringType(),
                              'zipcd': StringType(),
                              'count_in_network': IntegerType(),
                              'count_out_of_network': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page4_loyalty_map_pcps")

# COMMAND ----------

# page4_pcp_dist 

schema = create_empty_output({'npi_pcp': StringType(),
                              'specialty_cat_spec': StringType(),
                              'affiliation_4cat_pcp': StringType(),
                              'count_in_network': IntegerType(),
                              'count_out_of_network': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page4_pcp_dist")

# COMMAND ----------

# page4_patient_flow_pcps

schema = create_empty_output({'npi_pcp': StringType(),
                              'name_pcp': StringType(),
                              'npi_url_pcp': StringType(),
                              'specialty_cat_spec': StringType(),
                              'npi_spec': StringType(),
                              'name_spec': StringType(),
                              'npi_url_spec': StringType(),
                              'network_flag_spec': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page4_patient_flow_pcps")

# COMMAND ----------

# page4_net_leakage

schema = create_empty_output({'specialty_cat_spec': StringType(),
                              'net_defhc_id_spec': IntegerType(),
                              'net_defhc_name_spec': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page4_net_leakage")

# COMMAND ----------

# MAGIC %md #### F. Facilities (Page 5)

# COMMAND ----------

# page5_facility_map

schema = create_empty_output({'facility_id': IntegerType(),
                              'facility_name': StringType(),
                              'facility_type': StringType(),
                              'latitude': DoubleType(),
                              'longitude': DoubleType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page5_facility_map")

# COMMAND ----------

# page5_market_share

schema = create_empty_output({'facility_type': StringType(),
                              'network_label':  StringType(),
                              'network_name': StringType(),
                              'count': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page5_market_share")

# COMMAND ----------

# page5_top10_fac

schema = create_empty_output({'facility_type': StringType(),
                              'facility_id':  StringType(),
                              'facility_name': StringType(),
                              'network_flag': StringType(),
                              'count': IntegerType(),
                              'rank': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page5_top10_fac")

# COMMAND ----------

# page5_top10_pcp

schema = create_empty_output({'facility_type': StringType(),
                              'npi_pcp': StringType(),
                              'name_pcp':  StringType(),
                              'npi_url_pcp': StringType(),
                              'facility_id': StringType(),
                              'network_flag': StringType(),
                              'count': IntegerType(),
                              'rank': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page5_top10_pcp")

# COMMAND ----------

# page5_top10_postdis

schema = create_empty_output({'facility_id':  StringType(),
                              'discharge_facility_id':  StringType(),
                              'discharge_facility_name': StringType(),
                              'count': IntegerType(),
                              'rank': IntegerType()
                             })

pyspark_to_hive(schema, f"{DATABASE}.page5_top10_postdis")

# COMMAND ----------

exit_notebook(f"Initial setup run to create lookup tables: {', '.join(LOOKUP_TABLES)}",
              fail=False)
