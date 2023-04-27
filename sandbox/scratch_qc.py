# Databricks notebook source
# scratch notebook for assorted QC checks

# COMMAND ----------

from _general_funcs.sdf_print_comp_funcs import sdf_frequency

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Assorted checks to match dashboards

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select affiliation_4cat_pcp
# MAGIC        ,sum(case when loyalty = 'Loyal' then 1 else 0 end) as loyal
# MAGIC        ,sum(case when loyalty = 'Splitter' then 1 else 0 end) as splitter
# MAGIC        ,sum(case when loyalty = 'Dissenter' then 1 else 0 end) as dissenter
# MAGIC                ,count(*) as total
# MAGIC from (
# MAGIC 
# MAGIC select *
# MAGIC        ,case when count_in_network/(count_in_network+count_out_of_network) >= .7 then 'Loyal'
# MAGIC             when count_in_network/(count_in_network+count_out_of_network) >= .3 then 'Splitter'
# MAGIC             else 'Dissenter'
# MAGIC             end as loyalty
# MAGIC from (
# MAGIC   select npi_pcp, 
# MAGIC          affiliation_4cat_pcp, 
# MAGIC          sum(count_in_network) as count_in_network,
# MAGIC          sum(count_out_of_network) as count_out_of_network
# MAGIC 
# MAGIC 
# MAGIC    from ds_provider.page4_pcp_dist
# MAGIC   where defhc_id=547
# MAGIC   group by npi_pcp, 
# MAGIC          affiliation_4cat_pcp) a ) b
# MAGIC group by affiliation_4cat_pcp  
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from (
# MAGIC 
# MAGIC select name_pcp
# MAGIC        ,sum(case when network_flag_spec = 'In-Network' then count else 0 end) as in_network
# MAGIC        ,sum(case when network_flag_spec = 'Out-of-Network' then count else 0 end) as out_network
# MAGIC        ,sum(count) as tot
# MAGIC 
# MAGIC from ds_provider.page4_patient_flow_pcps
# MAGIC where defhc_id=547 and specialty_cat_spec in ('Vascular Surgery', 'Urology')
# MAGIC group by name_pcp )
# MAGIC order by tot desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Kauser Yasmeen

# COMMAND ----------

DB = 'ds_provider_2832'

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
# MAGIC ### Allen Detweiler

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

referrals = spark.sql(f"""
    select *
    from {DB}.pcp_referrals
    where npi_pcp = '{NPI}'
           
""")

# COMMAND ----------

sdf_frequency(referrals, ['rend_pos_cat'], with_pct=True)

# COMMAND ----------

referrals = referrals.withColumn('network_flag_3cat', F.when(F.col('net_defhc_id_spec').isNull(), 'No Network').otherwise(F.col('network_flag_spec')))

sdf_frequency(referrals, ['npi_spec', 'name_spec', 'network_flag_3cat', 'rend_pos_cat'], with_pct=True, maxobs=50)

# COMMAND ----------

sdf_frequency(referrals, ['net_defhc_id_spec'], with_pct=True)

# COMMAND ----------

sdf_frequency(referrals.filter((F.col('net_defhc_id_spec').isNotNull()) & (F.col('rend_pos_cat') != 'Office')), ['network_flag_spec'], with_pct=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Top 10 PCPs with three cat network values

# COMMAND ----------

spark.sql(f"select * from {DB}.input_org_info").display()

# COMMAND ----------

# NEW values

spark.sql(f"""
    select name_pcp
          ,tot
          ,100*(in/tot) as pct_in
          ,100*(out/tot) as pct_out
          ,100*(no/tot) as pct_no
    from (
    select name_pcp
          ,sum(case when network_flag_spec='Out-of-Network' then 1 else 0 end) as out
          ,sum(case when network_flag_spec='In-Network' then 1 else 0 end) as in
          ,sum(case when network_flag_spec='No Network' then 1 else 0 end) as no
          ,count(*) as tot
    from {DB}.pcp_referrals
    group by name_pcp
    ) a
        order by tot desc
    limit 10

""").display()

# COMMAND ----------

# OLD values

spark.sql(f"""
    select name_pcp
          ,tot
          ,100*(in/tot) as pct_in
          ,100*(out/tot) as pct_out
          ,100*(no/tot) as pct_no
    from (
    select name_pcp
          ,sum(case when network_flag_hco_spec='Out-of-Network' then 1 else 0 end) as out
          ,sum(case when network_flag_hco_spec='In-Network' then 1 else 0 end) as in
          ,sum(case when network_flag_hco_spec is null then 1 else 0 end) as no
          ,count(*) as tot
    from {DB}.pcp_referrals
    group by name_pcp
    ) a
        order by tot desc
    limit 10

""").display()
