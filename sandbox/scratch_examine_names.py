# Databricks notebook source
# scratch notebook to examine/clean long facility names

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

# COMMAND ----------

import re
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

# COMMAND ----------

names_sdf = spark.sql("""
    select hospital_name
           ,length(hospital_name) as length
           ,case when hospital_name like '%(%' or hospital_name like '%)%' then 1 else 0 end as has_parens
           ,case when upper(hospital_name) like '%(FKA%' then 1 else 0 end as fka
           ,case when upper(hospital_name) like '%(AKA%' then 1 else 0 end as aka
           ,case when upper(hospital_name) like '%CLOSED%' then 1 else 0 end as closed
           
    from definitivehc.hospital_all_companies

    """)

# COMMAND ----------

sdf_frequency(names_sdf, ['has_parens', 'fka', 'aka', 'closed'], with_pct=True, order='cols')

# COMMAND ----------

sdf_frequency(names_sdf, ['length'], order='cols', maxobs=200)

# COMMAND ----------

# look at those with parens but no other flag

names_sdf.filter((F.col('has_parens')==1) & (F.col('fka')==0) & (F.col('aka')==0) & (F.col('closed')==0)).display()

# all make sense

# COMMAND ----------

# look at those with very long names

names_sdf.filter(F.col('length')>100).sort('length').display()

# COMMAND ----------

names_pdf = names_sdf.toPandas()

# COMMAND ----------

def clean_name(name):
    
    # identify starting position with FKA or AKA
    fka_pos = name.find('(FKA')
    aka_pos = name.find('(AKA')
    
    start_pos = max(fka_pos, aka_pos)
    
    if start_pos > 0:
        
        # identify ending parens and if (TEMP/PERM) CLOSED in the name AND in between the parens
        end_pos = name.find(')', start_pos)
        
        closed_match = re.search('(TEMPORARILY|PERMANENTLY)?\s?CLOSED', name.upper())
        if closed_match:
        
            if closed_match.start() in range(start_pos, end_pos):
                # if CLOSED is in between parens, must KEEP parens and only remove up until beginning of CLOSED
                start_pos = start_pos+1
                end_pos = closed_match.start()-1
            
        return name[:start_pos] + name[end_pos+1:].strip()
    return name

# COMMAND ----------

names_pdf['hospital_name_cleaned'] = names_pdf['hospital_name'].apply(clean_name)

# COMMAND ----------

names_pdf.loc[(((names_pdf['fka']==1) | (names_pdf['aka']==1)) & (names_pdf['closed']==1))][['hospital_name','hospital_name_cleaned']].head(100)

# COMMAND ----------

names_pdf.loc[(((names_pdf['fka']==1) | (names_pdf['aka']==1)) & (names_pdf['closed']==0))][['hospital_name','hospital_name_cleaned']].head(50)

# COMMAND ----------

names_pdf.loc[(names_pdf['fka']==1) & (names_pdf['aka']==1)][['hospital_name','hospital_name_cleaned']].head(50)

# COMMAND ----------

# recreate length and look again at those > 100

names_pdf['length_cleaned'] = names_pdf['hospital_name_cleaned'].apply(lambda x: len(x))

# COMMAND ----------

names_pdf[names_pdf['length_cleaned']>100][['hospital_name','hospital_name_cleaned']].head(100)
