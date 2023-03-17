# Databricks notebook source
# scratch notebook to examine specific NPIs marked as high % of in/out-of-network for 

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from ds_provider.provider_oa_inputs_v1 where defhc_id=2832

# COMMAND ----------

DB = 'ds_provider_2832'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Kauser Yasmeen

# COMMAND ----------

NPI = 1679519656

# COMMAND ----------

# pull referrals for specific docs to examine: 
#   Kauser Yasmeen 1679519656, they think she is NOT loyal but chart shows she is?

# OVERALL, no affiliation subset

spark.sql(f"""
    select in+out as tot
          ,100*(in/(in+out)) as pct_in
          ,100*(out/(in+out)) as pct_out
    from (
    select sum(case when network_flag_spec='Out-of-Network' then 1 else 0 end) as out
          ,sum(case when network_flag_spec='In-Network' then 1 else 0 end) as in
    from {DB}.pcp_referrals
    where npi_pcp = '{NPI}'
    ) a

""").display()

# COMMAND ----------

# OVERALL, cardiology 

spark.sql(f"""
    select in+out as tot
          ,100*(in/(in+out)) as pct_in
          ,100*(out/(in+out)) as pct_out
    from (
    select sum(case when network_flag_spec='Out-of-Network' then 1 else 0 end) as out
          ,sum(case when network_flag_spec='In-Network' then 1 else 0 end) as in
    from {DB}.pcp_referrals
    where npi_pcp = '{NPI}'  and specialty_cat_spec = 'Cardiology'
    ) a

""").display()

# COMMAND ----------

# OVERALL, affiliation = primary

spark.sql(f"""
    select in+out as tot
          ,100*(in/(in+out)) as pct_in
          ,100*(out/(in+out)) as pct_out
    from (
    select sum(case when network_flag_spec='Out-of-Network' then 1 else 0 end) as out
          ,sum(case when network_flag_spec='In-Network' then 1 else 0 end) as in
    from {DB}.pcp_referrals
    where npi_pcp = '{NPI}'  and affiliation_2cat_spec = 'Primary'
    ) a

""").display()

# COMMAND ----------

# CARDIOLOGY ONLY, affiliation = primary

spark.sql(f"""
    select in+out as tot
          ,100*(in/(in+out)) as pct_in
          ,100*(out/(in+out)) as pct_out
    from (
    select sum(case when network_flag_spec='Out-of-Network' then 1 else 0 end) as out
          ,sum(case when network_flag_spec='In-Network' then 1 else 0 end) as in
    from {DB}.pcp_referrals
    where npi_pcp = '{NPI}' and specialty_cat_spec = 'Cardiology' and affiliation_2cat_spec = 'Primary'
    ) a

""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Allen Detweiler

# COMMAND ----------

NPI = 1992706402

# COMMAND ----------

# pull referrals for specific docs to examine: 
#   Allen Detweiler 1992706402, they think he IS loyal but chart shows he is not

# OVERALL, no affiliation subset

spark.sql(f"""
    select in+out as tot
          ,100*(in/(in+out)) as pct_in
          ,100*(out/(in+out)) as pct_out
    from (
    select sum(case when network_flag_spec='Out-of-Network' then 1 else 0 end) as out
          ,sum(case when network_flag_spec='In-Network' then 1 else 0 end) as in
    from {DB}.pcp_referrals
    where npi_pcp = '{NPI}'
    ) a

""").display()

# COMMAND ----------

# read in ALL referrals for him to identify where come from

START_DATE='2021-11-01'
END_DATE='2022-10-31'

referrals = spark.sql(f"""
    select *
    from {DB}.pcp_referrals
    where npi_pcp = '{NPI}'
           
""").checkpoint()

# COMMAND ----------

sdf_frequency(referrals, ['rend_pos_cat'], with_pct=True)

# COMMAND ----------

sdf_frequency(referrals, ['net_defhc_id_spec'], with_pct=True)

# COMMAND ----------

sdf_frequency(referrals.filter((F.col('net_defhc_id_spec').isNotNull()) & (F.col('rend_pos_cat') != 'Office')), ['network_flag_spec'], with_pct=True)
