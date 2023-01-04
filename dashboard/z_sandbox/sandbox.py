# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Sandbox for ad-hoc queries/runs for provider work

# COMMAND ----------

import pandas as pd

# COMMAND ----------

BASE_DIR = '/dbfs/FileStore/datascience/rmalsberger/provider'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check client IDs

# COMMAND ----------

df = pd.read_excel(BASE_DIR + '/ClientsWithDefID.xlsx')

df = df.loc[df['Definitive ID'] == df['Definitive ID']]
df['defhc_id'] = df['Definitive ID'].apply(lambda x: int(x))
df.head()

# COMMAND ----------

id_df = spark.sql(f"""
    select hospital_name as defhc_name
        ,  network_id as input_network
        ,  hospital_id as defhc_id
        , case when network_id is not null then 1 else 0 end as has_network
    from   definitivehc.hospital_all_companies
    """).toPandas()

joined = pd.merge(df, id_df, left_on='defhc_id', right_on='defhc_id', how='left')
joined.head()

# COMMAND ----------

joined.to_csv(BASE_DIR + '/provider_networks.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Specialty designations

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select specialty_name
# MAGIC        ,category_name
# MAGIC        ,case when pcp=1 then 'PCP'
# MAGIC              when pcp=0 then 'Specialist'
# MAGIC              else 'None'
# MAGIC              end as designation
# MAGIC 
# MAGIC 
# MAGIC from hcp_specialties.ref_specialty_ids a 
# MAGIC      full join
# MAGIC      ds_provider.pcp_spec_assign b
# MAGIC      
# MAGIC     on a.specialty_name = b.specialty
# MAGIC     
# MAGIC     left join 
# MAGIC     hcp_specialties.ref_specialty_category_ids c
# MAGIC     
# MAGIC     on a.primary_category_id = c.category_id;
