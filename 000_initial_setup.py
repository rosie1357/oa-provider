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
# MAGIC **Outputs** (Lookup tables):
# MAGIC   - {DATABASE}.pcp_spec_assign
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

import pandas as pd

from functools import reduce
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

RUN_VALUES = get_widgets(include_widgets=[5])

DATABASE = return_widget_values(RUN_VALUES, ['DATABASE'])[0]

# COMMAND ----------

# list of lookup tables to create

LOOKUP_TABLES = [f"{DATABASE}.pcp_spec_assign",
                 f"{DATABASE}.pos_category_assign"]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Create pcp vs specialist lookup tables

# COMMAND ----------

# create lookup table with specialist types and specialist/PCP flag

specialist_list = ['Allergy/Immunology'
               , 'Audiology'
               , 'Cardiac Electrophysiology'
               , 'Cardiac Surgery'
               , 'Cardiology'
               , 'Clinical Neuropsychology'
               , 'Colorectal Surgery'
               , 'Dermatology'
               , 'Endocrinology'
               , 'Foot and Ankle Surgery'
               , 'Gastroenterology'
               , 'General Surgery'
               , 'Gynecological Oncology'
               , 'Hand Surgery'
               , 'Hematology'
               , 'Infectious Disease'
               , 'Interventional Cardiology'
               , 'Medical Oncology'
               , 'Nephrology'
               , 'Neurology'
               , 'Neurosurgery'
               , 'Obstetrics/Gynecology'
               , 'Oncology'
               , 'Oral and Maxillofacial Surgery'
               , 'Orthopedic Surgery'
               , 'Otolaryngology'
               , 'Pediatric Allergy/Immunology'
               , 'Pediatric Cardiology'
               , 'Pediatric Endocrinology'
               , 'Pediatric Gastroenterology'
               , 'Pediatric Infectious Disease'
               , 'Pediatric Nephrology'
               , 'Pediatric Neurology'
               , 'Pediatric Oncology'
               , 'Pediatric Orthopedic Surgery'
               , 'Pediatric Otolaryngology'
               , 'Pediatric Pulmonology'
               , 'Pediatric Rheumatology'
               , 'Pediatric Surgery'
               , 'Pediatric Urology'
               , 'Plastic and Reconstructive Surgery'
               , 'Podiatry'
               , 'Pulmonology'
               , 'Radiation Oncology'
               , 'Reproductive Endocrinology'
               , 'Rheumatology'
               , 'Surgical Oncology'
               , 'Thoracic Surgery'
               , 'Trauma Surgery'
               , 'Urology'
               , 'Vascular Surgery']


pcp_list = ['Internal Medicine'
          , 'Pediatric Medicine'
          , 'Family Practice'
          , 'Adult Medicine'
          , 'Adolescent Medicine'
          , 'Geriatric Medicine']

# COMMAND ----------

# create df with pcp flag

df = pd.concat([pd.DataFrame(specialist_list, columns = ['specialty']).assign(pcp=0),
                pd.DataFrame(pcp_list, columns = ['specialty']).assign(pcp=1)
               ])

# COMMAND ----------

# save to output table in database

pyspark_to_hive(spark.createDataFrame(df),
               LOOKUP_TABLES[0])

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
# MAGIC #### 3A. Dashboard (Page 1)

# COMMAND ----------

# page1_toplevel_counts

schema = create_empty_output({'cnt_patients': IntegerType(),
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

exit_notebook(f"Initial setup run to create lookup tables: {', '.join(LOOKUP_TABLES)}",
              fail=False)
