# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Sandbox notebook to explore claims and referrals subsets

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

# COMMAND ----------

DATABASE = 'ds_provider'
TMP_DATABASE = GET_TMP_DATABASE(DATABASE)

# COMMAND ----------

def print_summary(sdf):
    
    summ_sdf = sdf.summary()
    
    for col in (col for col in summ_sdf.columns if col != 'summary'):
        summ_sdf = summ_sdf.withColumn(col, F.format_number(F.col(col).cast('double'),1))
    
    return summ_sdf

# COMMAND ----------

def rollup_provider(defhc_id, radius):
    
    sdf = spark.sql(f"""
    
    select RenderingProviderNPI
           ,100 * (sum(case when nearby_fac=1 then 1 else 0 end) / sum(1)) as pct_nearby
           ,100 * (sum(case when nearby_fac=0 then 1 else 0 end) / sum(1)) as pct_not_nearby
           ,100 * (sum(case when nearby_fac is null then 1 else 0 end) / sum(1)) as pct_unknown
           ,sum(1) as tot_claims
           
     from {TMP_DATABASE}.{MX_CLMS_TBL}
          where nearby_prov=1
          group by RenderingProviderNPI

    """)
    
    print(f"Stats for nearby providers for DEFHC_ID = {defhc_id} and RADIUS = {radius}")

    print_summary(sdf.drop('RenderingProviderNPI')).display()

# COMMAND ----------

def rollup_facility(defhc_id, radius):
    
    sdf = spark.sql(f"""
    
    select coalesce(FacilityNPI, BillingProviderNPI) as fac_npi
           ,100 * (sum(case when nearby_prov=1 then 1 else 0 end) / sum(1)) as pct_nearby
           ,100 * (sum(case when nearby_prov=0 then 1 else 0 end) / sum(1)) as pct_not_nearby
           ,100 * (sum(case when nearby_prov is null then 1 else 0 end) / sum(1)) as pct_unknown
           ,sum(1) as tot_claims
           
     from {TMP_DATABASE}.{MX_CLMS_TBL}
          where nearby_fac=1
          group by coalesce(FacilityNPI, BillingProviderNPI)

    """)
    
    print(f"Stats for nearby facilities for DEFHC_ID = {defhc_id} and RADIUS = {radius}")

    print_summary(sdf.drop('fac_npi')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1. Stats for Fairbanks (DEFHC_ID = 126) with RADIUS=25

# COMMAND ----------

# roll up claims to provider, get % of claims nearby

rollup_provider(126, 25)

# COMMAND ----------

# roll up claims to facility, get % of claims nearby

rollup_facility(126, 25)

# COMMAND ----------

# look into specific facilities for non-nearby providers

prov_rollup_sdf = spark.sql(f"""
    
    select   RenderingProviderNPI 
            ,  BillingProviderNPI 
            ,  FacilityNPI 

            ,  zip 
            ,  defhc_id 
            ,  net_defhc_id
            ,  defhc_name
            ,  net_defhc_name
            ,  network_flag
            ,  facility_type

            , PrimarySpecialty
            , ProviderName
            , specialty_cat
            , specialty_type
            , nearby_prov

            , provider_primary_affiliation_id
            , affiliated_flag
            
            ,sum(1) as count
            
    from {TMP_DATABASE}.{MX_CLMS_TBL}
         where nearby_fac=1 and
               nearby_prov=0
   
   group by RenderingProviderNPI 
            ,  BillingProviderNPI 
            ,  FacilityNPI 

            ,  zip 
            ,  defhc_id 
            ,  net_defhc_id
            ,  defhc_name
            ,  net_defhc_name
            ,  network_flag
            ,  facility_type

            , PrimarySpecialty
            , ProviderName
            , specialty_cat
            , specialty_type
            , nearby_prov

            , provider_primary_affiliation_id
            , affiliated_flag
            
    order by count desc

""").display()

# COMMAND ----------

# pull all claims for specific non-nearby prov

claims_sdf = spark.sql(f"""
    select  RenderingProviderNPI 
           , BillingProviderNPI 
           , FacilityNPI 
           , sum(1) as count
           
    from MxMart.F_MxClaim
    where to_date(cast(MxClaimDateKey as string), 'yyyyMMdd') between '2021-01-01' and '2021-12-31' and
           MxClaimYear >= 2016 and
           MxClaimMonth between 1 and 12 and
           RenderingProviderNPI = 1801201678

    group by RenderingProviderNPI 
           , BillingProviderNPI 
           , FacilityNPI
           
    order by count desc

    """)

claims_sdf.display()

# COMMAND ----------


