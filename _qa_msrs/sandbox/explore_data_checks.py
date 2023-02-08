# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Notebook to explore various data inconsistences

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1. Dups in referrals for 2830

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from (
# MAGIC 
# MAGIC   select *
# MAGIC          ,count(*) over (partition by rend_claim_id) as nrecs
# MAGIC 
# MAGIC   from ds_provider_2830.pcp_referrals
# MAGIC   ) where nrecs>1

# COMMAND ----------

dups = spark.sql('select rend_claim_id, count(*) as count from ds_provider_2830.pcp_referrals group by rend_claim_id')

dups.filter(F.col('count')>1).createOrReplaceTempView('dups_vw')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select a.*
# MAGIC 
# MAGIC from (select  'explicit' as source
# MAGIC                ,   rend_claim_id
# MAGIC                ,   ref_NPI 
# MAGIC                ,   rend_NPI 
# MAGIC                ,   rend_claim_date
# MAGIC                ,   patient_id 
# MAGIC                ,   coalesce(rend_fac_npi, rend_bill_npi) as rend_fac_npi
# MAGIC                ,   coalesce(ref_fac_npi, ref_bill_npi) as ref_fac_npi
# MAGIC                ,   rend_pos
# MAGIC             from   ds_provider.explicit_referrals 
# MAGIC             where  rend_claim_date between '2021-10-01' and '2022-09-30'
# MAGIC 
# MAGIC             union  distinct 
# MAGIC 
# MAGIC             select 'implicit' as source
# MAGIC                ,   rend_claim_id
# MAGIC                ,   ref_NPI 
# MAGIC                ,   rend_NPI 
# MAGIC                ,   rend_claim_date
# MAGIC                ,   patient_id 
# MAGIC                ,   coalesce(rend_fac_npi, rend_bill_npi) as rend_fac_npi
# MAGIC                ,   coalesce(ref_fac_npi, ref_bill_npi) as ref_fac_npi
# MAGIC                ,   rend_pos
# MAGIC             from   ds_provider.implicit_referrals_pcp_specialist
# MAGIC             where  rend_claim_date between '2021-10-01' and '2022-09-30'
# MAGIC         
# MAGIC         ) a 
# MAGIC      inner join
# MAGIC      dups_vw b
# MAGIC      
# MAGIC      on a.rend_claim_id = b.rend_claim_id
# MAGIC      
# MAGIC order by rend_claim_id
