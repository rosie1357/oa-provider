# Databricks notebook source
# scratch notebook for assorted QC checks

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

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

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select net_defhc_name_spec, sum(count) as count
# MAGIC 
# MAGIC from ds_provider.page4_net_leakage
# MAGIC where defhc_id=547
# MAGIC group by net_defhc_name_spec

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

INPUT_NETWORK = 7191

# COMMAND ----------

hcos_npi = spark.sql(f"""

    select *
          , {network_flag('net_defhc_id', INPUT_NETWORK)}
          , case when net_defhc_id is null then 1 else 0 end as no_network
          
    from (

        select cast(o.NPI as int) as NPI
             , p.DefinitiveId as defhc_id 
             , p.NetworkDefinitiveId
                    
             , p.NetworkName
             , p.NetworkParentDefinitiveID
             , p.NetworkParentName

             , p.FirmTypeName
             , {assign_fac_types(alias='p')}

             , case when p.NetworkDefinitiveId = {INPUT_NETWORK} or
                         p.NetworkParentDefinitiveID = {INPUT_NETWORK}
                    then {INPUT_NETWORK}
                    
                    else coalesce(p.NetworkParentDefinitiveID, p.NetworkDefinitiveId)
                    end as net_defhc_id
                    
            , case when p.NetworkDefinitiveId = {INPUT_NETWORK} then concat('*', p.NetworkName) 
                   when p.NetworkParentDefinitiveID = {INPUT_NETWORK} then concat('*', p.NetworkParentName) 
            
                   else coalesce(p.NetworkParentName, p.NetworkName)
                   end as net_defhc_name

         from  MartDim.D_Organization o 

         left   join MartDim.D_Profile p 
         on     o.DefinitiveId = p.DefinitiveId

         where o.ActiveRecordInd is True
         ) a
           
   """)

hcos_npi.createOrReplaceTempView('hcos_npi_base_vw')

# COMMAND ----------

# look at unique facilities w/wout network by firm type

facilities_sdf = spark.sql("""
    
    select *
         ,100*(cnt_no_network/total) as pct_no_network
         
   from (

        select firm_type
               ,sum(case when coalesce(network_parent_id, network_id) is not null then 1 else 0 end) as cnt_network
               ,sum(case when coalesce(network_parent_id, network_id) is null then 1 else 0 end) as cnt_no_network
               ,count(*) as total
       from definitivehc.hospital_all_companies
       group by firm_type
    ) a

""")

facilities_sdf.display()

# COMMAND ----------

hcp_affs = spark.sql(f"""

    select a.*
           ,b.network_flag
           ,b.net_defhc_id as net_defhc_id_spec2
           ,b.FirmTypeName
    
    from (

        select physician_npi as npi_spec
               ,defhc_id
               ,hospital_name
               ,row_number() over (partition by physician_npi 
                                   order by score_bucket desc, score desc)
                             as rn

                from   hcp_affiliations.physician_org_affiliations
                where  include_flag = 1 and
                       current_flag = 1

           ) a
     
     left join
     
     (select distinct defhc_id, net_defhc_id, FirmTypeName, network_flag from hcos_npi_base_vw) b
         
     on a.defhc_id = b.defhc_id
     
     where a.rn=1
   """)

hcp_affs.createOrReplaceTempView('aff_vw')

# COMMAND ----------

referrals_top10 = spark.sql(f"""

    select *
           ,100*(cnt_in_network / total) as pct_in_network
           ,100*(cnt_out_of_network / total) as pct_out_of_network
           ,100*(cnt_no_network / total) as pct_no_network
           
           ,100*(cnt_in_network2 / total) as pct_in_network2
           ,100*(cnt_out_of_network2 / total) as pct_out_of_network2
           ,100*(cnt_no_network2 / total) as pct_no_network2
           
   from (

       select  name_pcp
               ,npi_pcp

               ,sum(case when network_flag_spec = 'In-Network' then 1 else 0 end) as cnt_in_network
               ,sum(case when network_flag_spec = 'Out-of-Network' and net_defhc_id_spec is not null then 1 else 0 end) as cnt_out_of_network
               ,sum(case when net_defhc_id_spec is null then 1 else 0 end) as cnt_no_network


                ,sum(case when (network_flag_spec = 'In-Network') or (net_defhc_id_spec is null and network_flag_spec2 = 'In-Network') then 1 else 0 end) as cnt_in_network2
               ,sum(case when (network_flag_spec = 'Out-of-Network' and net_defhc_id_spec is not null) or (net_defhc_id_spec is null and network_flag_spec2 = 'Out-of-Network' and net_defhc_id_spec2 is not null)  then 1 else 0 end) as cnt_out_of_network2
               ,sum(case when net_defhc_id_spec is null and net_defhc_id_spec2 is null then 1 else 0 end) as cnt_no_network2

               ,count(*) as total

        from (
        select name_pcp
               ,npi_pcp
               ,y.npi_spec 
               ,network_flag_spec
               ,net_defhc_id_spec
               ,y.network_flag as network_flag_spec2
               ,y.net_defhc_id_spec2


        from {DB}.pcp_referrals x 
        left join 
        aff_vw y
        on x.npi_spec = y.npi_spec

        ) a
        group by name_pcp
               ,npi_pcp
    
    ) b
    
    order by total desc
    limit 10
           
""")

# COMMAND ----------

referrals_top10.display()

# COMMAND ----------

# now look by PCP and specialist to extract specialists with no network

referrals_top10 = spark.sql(f"""

    select *
    
    from (

        select *
               ,100*(cnt_in_network / total) as pct_in_network
               ,100*(cnt_out_of_network / total) as pct_out_of_network
               ,100*(cnt_no_network / total) as pct_no_network

               ,row_number() over (partition by npi_pcp order by total desc) as rn

       from (

           select  name_pcp
                   ,npi_pcp
                   ,npi_spec
                   ,name_spec
                   ,specialty_cat_spec
                   ,network_flag_spec2
                   ,net_defhc_id_spec2
                   ,defhc_id_spec2
                   ,FirmTypeName

                   ,sum(case when network_flag_spec2 = 'In-Network' then 1 else 0 end) as cnt_in_network
                   ,sum(case when network_flag_spec2 = 'Out-of-Network' and net_defhc_id_spec2 is not null then 1 else 0 end) as cnt_out_of_network
                   ,sum(case when net_defhc_id_spec2 is null then 1 else 0 end) as cnt_no_network


                   ,count(*) as total

            from (
            select name_pcp
                   ,npi_pcp
                   ,name_spec
                   ,specialty_cat_spec

                   ,x.npi_spec 
                   ,network_flag_spec
                   ,net_defhc_id_spec
                   ,y.network_flag as network_flag_spec2
                   ,y.net_defhc_id_spec2
                   ,y.defhc_id as defhc_id_spec2
                   ,y.FirmTypeName


            from {DB}.pcp_referrals x 
            left join 
            aff_vw y
            on x.npi_spec = y.npi_spec
            
            where npi_pcp in ('1295796464', '1477569416', '1821368390', '1992706402') and net_defhc_id_spec is null

            ) a
            group by name_pcp
                   ,npi_pcp
                   ,npi_spec
                   ,name_spec
                   ,specialty_cat_spec
                   ,network_flag_spec2
                   ,net_defhc_id_spec2
                   ,defhc_id_spec2
                   ,FirmTypeName

        ) b
    )c
    where rn <= 20 and net_defhc_id_spec2 is not null
    
           
""")

# COMMAND ----------

referrals_top10.sort('npi_pcp','rn').display()

# COMMAND ----------

spark.sql(f"""

    select a.*
           ,b.network_flag
           ,b.net_defhc_id
           ,b.net_defhc_name
    
    from (

        select *
               ,row_number() over (partition by physician_npi
                                   order by score_bucket desc, score desc)
                             as rn

                from   hcp_affiliations.physician_org_affiliations
                where  include_flag = 1 and
                       current_flag = 1 and physician_npi=1568422913

           ) a
     
     left join
     
     (select distinct defhc_id, net_defhc_id, net_defhc_name, network_flag from hcos_npi_base_vw) b
         
     on a.defhc_id = b.defhc_id
     
   """).display()
