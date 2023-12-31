# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 001 Create Tables
# MAGIC 
# MAGIC **Program:** 001_create_tables
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to get metrics for input ID and create all base tables to save as tmp for later notebooks <br>
# MAGIC <br>
# MAGIC 
# MAGIC **NOTE**: DATABASE param below is value extracted from database widget, FAC_DATABASE is assigned in ProviderRunClass
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {DATABASE}.hcp_specialty_assignment
# MAGIC   - {DATABASE}.pos_category_assign
# MAGIC   - definitivehc.hospital_all_companies
# MAGIC   - npi_hco_mapping.dhc_pg_location_addresses
# MAGIC   - definitivehc.hospital_physician_compare_physicians
# MAGIC   - npi_hco_mapping.npi_registry_addresses
# MAGIC   - hcp_affiliations.physician_org_affiliations
# MAGIC   - MxMart.F_MxClaim
# MAGIC   - MartDim.D_Organization
# MAGIC   - MartDim.D_Profile
# MAGIC   - MartDim.D_Provider
# MAGIC   - {DATABASE}.explicit_referrals
# MAGIC   - {DATABASE}.implicit_referrals_pcp_specialist
# MAGIC   
# MAGIC **Outputs** (inserted into):
# MAGIC   - {FAC_DATABASE}.input_org_info
# MAGIC   - {FAC_DATABASE}.nearby_hcos_id
# MAGIC   - {FAC_DATABASE}.nearby_hcps
# MAGIC   - {FAC_DATABASE}.nearby_hcos_npi
# MAGIC   - {FAC_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {FAC_DATABASE}.{PCP_REFS_TBL}
# MAGIC   - {FAC_DATABASE}.inpat90_dashboard
# MAGIC   - {FAC_DATABASE}.inpat90_facilities

# COMMAND ----------

# MAGIC %run ./_funcs/_paths_include

# COMMAND ----------

from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

from _funcs.setup_funcs import get_widgets, return_widget_values
from _funcs.ProviderRunClass import ProviderRun
from _funcs.params import network_flag, assign_fac_types, PHYS_LINK, CHECKPOINT_DIR, MX_CLMS_TBL, PCP_REFS_TBL, NEW_STAY_DAYS_CUTOFF
from _funcs.geo_funcs import get_intersection, get_coordinates

from _general_funcs.base_python_funcs import add_time
from _general_funcs.sdf_funcs import sdf_return_row_values, sdf_create_window, add_null_indicator
from _general_funcs.sdf_print_comp_funcs import sdf_frequency
from _general_funcs.fs_funcs import rm_checkpoints, hive_to_df

# COMMAND ----------

# set checkpoint dir 

spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

# COMMAND ----------

# create all widgets

RUN_VALUES = get_widgets()

# COMMAND ----------

# get widget values and use to create instance of provider run class

DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'SUBSET_LT18', 'DATABASE', 'RUN_QC'])

ProvRunInstance = ProviderRun(DEFHC_ID, RADIUS, START_DATE, END_DATE, SUBSET_LT18, DATABASE, RUN_QC, base_output_prefix='input_', charts_instance=False)

# COMMAND ----------

# to pull in claims for 90-days post-inpatient stay, create END_DATE + 90 days

END_DATE_P90 = add_time(END_DATE, add_days=90)

print(f"START_DATE = {START_DATE}, END_DATE = {END_DATE}, END_DATE + 90 DAYS = {END_DATE_P90}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Org Info and Affiliations View

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1A. Get input org info

# COMMAND ----------

input_org_info = spark.sql(f"""
    select hospital_name as defhc_name
        ,  coalesce(network_parent_id, network_id) as input_network
        ,  firm_type
        ,  trim(concat(ifnull(hq_address, ''), ' ', ifnull(hq_address1, ''))) as address 
        ,  hq_city as defhc_city
        ,  hq_state as defhc_state
        ,  hq_zip_code as defhc_zip
        ,  hq_latitude as defhc_lat
        ,  hq_longitude as defhc_long
    from   definitivehc.hospital_all_companies 
    where  hospital_id = {DEFHC_ID}""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{ProvRunInstance.fac_database}.input_org_info"

ProvRunInstance.create_final_output(input_org_info, table=TBL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1Ai. Initial checks for compliance

# COMMAND ----------

# create variables with input network and firm type:
#   if no record found at all, exit with error no matching defhc_id
#   if no network found, exit with error no parent network

try:
    INPUT_NETWORK, FIRM_TYPE = sdf_return_row_values(input_org_info, ['input_network', 'firm_type'])
    
except IndexError:
    ProvRunInstance.exit_notebook(fail_message = f"Input Definitive ID {DEFHC_ID} not found in database")

if INPUT_NETWORK is None:
    ProvRunInstance.exit_notebook(fail_message = f"Input Definitive ID {DEFHC_ID} is missing parent network")
    
print(f"Input network: {INPUT_NETWORK}")
print(f"Input firm type: {FIRM_TYPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1B. Create temp view of all HCO NPIs with IDs/networks

# COMMAND ----------

"""
  join all active org NPIs to d_profile to get defhc_id and network def_hcid
  to determine network to assign, we will use a two-step process:
      -- if EITHER NetworkDefinitiveId OR NetworkParentDefinitiveID (network or parent network) == input network, assign input network
      -- otherwise, assign network as coalesce(NetworkParentDefinitiveID, NetworkDefinitiveId), to prioritize parent if assigned, otherwise use network
      
  for given facility and given network, surround facility/network name in asterisks

"""

hcos_npi = spark.sql(f"""

    select *
          , {network_flag('net_defhc_id', INPUT_NETWORK)}
          
    from (

        select cast(o.NPI as int) as NPI
             , p.DefinitiveId as defhc_id 
             , p.NetworkDefinitiveId
             , case when p.DefinitiveId = {DEFHC_ID} then concat('*', p.ProfileName) 
                    else p.ProfileName
                    end as defhc_name
                    
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

# confirm unique by npi

ProvRunInstance.test_distinct(sdf = hcos_npi,
                              name = 'all_hcos_npi',
                              cols = ['npi'],
                              to_subset = False
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1C. Create temp view of all HCPs with IDs/affiliations/specialty

# COMMAND ----------

# read in affiliations to identify for each NPI:
#  -- TOP affiliation overall,
#  -- TOP affiliation for given firm type,
#  --  whether ANY affiliation is equal to given facility

hcp_affs = spark.sql(f"""

    select physician_npi
           
           /* identify top facility overall (used for assigning network if otherwise missing) */
           
           ,max(case when rn=1 then defhc_id else null end) as defhc_id_primary_overall
           
           /* identify top facility for given firm type, and whether primary or any for given firm type is given facility */
           
           ,max(case when rn_firm_type=1 and firm_type='{FIRM_TYPE}' then defhc_id else null end) as defhc_id_primary
           ,max(case when rn_firm_type=1 and firm_type='{FIRM_TYPE}' then hospital_name else null end) as defhc_name_primary
           
           ,max(case when rn_firm_type=1 and firm_type='{FIRM_TYPE}' and defhc_id={DEFHC_ID} then 1 else 0 end) as primary_affiliation
           ,max(case when rn_firm_type>1 and firm_type='{FIRM_TYPE}' and defhc_id={DEFHC_ID} then 1 else 0 end) as secondary_affiliation
           
    from (
    
        select physician_npi
               ,defhc_id
               ,hospital_name
               ,firm_type
               ,row_number() over (partition by physician_npi
                                   order by score_bucket desc, score desc)
                             as rn
                             
               ,row_number() over (partition by physician_npi, firm_type
                                   order by score_bucket desc, score desc)
                             as rn_firm_type

                from   hcp_affiliations.physician_org_affiliations
                where  include_flag = 1 and
                       current_flag = 1
                       
           ) a
           
     group by physician_npi
   """)

hcp_affs.createOrReplaceTempView('hcp_affs_vw')

# COMMAND ----------

"""
 join above table at NPI-level to hcos_npi_base_vw TWICE:
     - to get network for overall primary affiliation
     - to create affiliation categories for firm type-specific primary affiliation 
 
 For firm type-specific primary affiliation, want to identify whether top affiliation
 for each provider is in network, and create 2 and 4 category affiliations
 
 two category affiliation:
   1. Primary affiliation to given facility
   2. Secondary affiliation to given facility

four category affiliation:
   1. Primary affiliation to given facility
   2. Primary affiliation to different facility but in network
   3. Primary affiliation to different facility outside of network
   4. Primary affiliation unknown: NOTE, this assignment can only be done in a later step 
          because all HCPs in this table will have a non-null affiliation

"""

hcp_affs_net = spark.sql("""
    
    select a.*
          ,b.net_defhc_name as net_defhc_name_hcp
          ,b.net_defhc_id as net_defhc_id_hcp
          ,b.network_flag as network_flag_hcp
        
          ,case when primary_affiliation=1 then 'Primary'
                when secondary_affiliation=1 then 'Secondary'
                else null
                end as affiliation_2cat

          ,case when primary_affiliation=1 then 'Facility' 
                when c.network_flag = 'In-Network' then 'In-Network'
                when defhc_id_primary is not null then 'Competitor'
                else null
                end as affiliation_4cat
    
    from hcp_affs_vw a
         left join
         (select distinct defhc_id, net_defhc_id, net_defhc_name, network_flag from hcos_npi_base_vw) b
         
         on a.defhc_id_primary_overall = b.defhc_id
     
         left join
         (select distinct defhc_id, network_flag from hcos_npi_base_vw) c
         
         on a.defhc_id_primary = c.defhc_id
     
    """)

hcp_affs_net.createOrReplaceTempView('hcp_affs_net_vw')

# COMMAND ----------

# take martdim.d_provider as source of truth for ALL individual NPIs, and join on other NPI-info including affiliations created above
# for those with null/no specialty assignment, set specialty columns according to those with 'Other' (unknown) specialty

hcps = spark.sql(f"""

    select cast(pv.NPI as int) as npi
          ,coalesce(pv.PrimarySpecialty, 'Other') as PrimarySpecialty
          ,pv.ProviderName
          
          ,coalesce(sp.specialty_cat, 'Other') as specialty_cat
          ,coalesce(sp.specialty_type, 'None') as specialty_type
          ,coalesce(sp.include_pie, 'N') as include_pie
          
          ,aff.net_defhc_name_hcp
          ,aff.net_defhc_id_hcp
          ,aff.network_flag_hcp
          ,aff.defhc_id_primary
          ,aff.defhc_name_primary
          
          ,aff.affiliation_2cat
          ,coalesce(aff.affiliation_4cat, 'Independent') as affiliation_4cat
          
          ,cp.hq_zip_code as zip
          ,concat("{PHYS_LINK}", pv.npi) as npi_url
          
    from martdim.d_provider pv

          left join   hcp_affs_net_vw aff
          on          pv.NPI = aff.physician_npi

          left join  {DATABASE}.hcp_specialty_assignment sp
          on         pv.PrimarySpecialty = sp.specialty_name
          
          left join  definitivehc.hospital_physician_compare_physicians cp
          on         pv.NPI = cp.npi

""").checkpoint()

hcps.createOrReplaceTempView('hcps_base_vw')

# COMMAND ----------

# confirm unique by npi

ProvRunInstance.test_distinct(sdf = hcps,
                              name = 'all_hcps',
                              cols = ['npi'],
                              to_subset = False
                              )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Find nearly facilities

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### 2A. All Provider Coordinates
# MAGIC 
# MAGIC HCOs: from all_companies <br>
# MAGIC HCPs: from physician_compare_physicians master table <br>
# MAGIC NPIs: from NPI Registry

# COMMAND ----------

# Latitude & Longitude by DEFHC_ID; includes PG locations 
# This data is complete, i.e. every location has a longitude and latitude 

df_hco_profile_coords = spark.sql(f"""
                                select hospital_id as defhc_id 
                                    ,  hq_longitude as longitude
                                    ,  hq_latitude as latitude
                                    ,  hq_zip_code as zip
                                    ,  1 as primary
                                from   definitivehc.hospital_all_companies 
                                
                                union  distinct 
                                
                                select defhc_id
                                    ,  hq_longitude
                                    ,  hq_latitude 
                                    ,  facility_zip
                                    ,  0 as primary
                                from   npi_hco_mapping.dhc_pg_location_addresses
                                """).toPandas()

# COMMAND ----------

# Latitude & Longitude by TYPE 1 NPI (only those tracked as physicians)
# This data is complete, i.e. every physician has a longitude and latitude 

df_hcp_profile_coords = spark.sql(f"""
                                select cast(NPI as int) as NPI
                                    ,  longitude
                                    ,  latitude
                                    ,  hq_zip_code as zip
                                from   definitivehc.hospital_physician_compare_physicians
                                """).toPandas()

# COMMAND ----------

# Latitude & Longitude by TYPE 2 NPI (from NPI registry)
# This data may not be complete, as it relies on the geocoding processes from DB engineering and Data Science 
# Some addresses fail geocoding 
# Default to the longitude and latitude of the main location from the defhc profile if not available at the NPI level 
# It is preferable to use the geo of the NPI when using claims data especially for profiles that have multiple locations (PGs)

# QUICK FIX!! the table npi_hco_mapping.ds_geocoded_addresses currently has multiple records per address
# until this is fixed, take first record by lat/long per address with non-null lat/long

geo_addrs = spark.sql("""

    select address_to_geo
           ,latitude
           ,longitude
           ,row_number() over (partition by address_to_geo
                               order by latitude, longitude )
                         as rn
           
   from npi_hco_mapping.ds_geocoded_addresses
   where latitude is not null

""")

geo_addrs.filter(F.col('rn')==1).createOrReplaceTempView('geo_addrs_vw')


df_hco_npi_coords = spark.sql(f"""
                            select cast(na.NPI as int) as NPI
                                ,  ifnull(geo.longitude, ac.hq_longitude) as longitude
                                ,  ifnull(geo.latitude, ac.hq_latitude) as latitude 
                                ,  zip_code as zip 
                                ,  m.defhc_id
                                
                            from   npi_hco_mapping.npi_registry_addresses na 
                                   left   join geo_addrs_vw geo
                                   on     na.address_to_geo = geo.address_to_geo 
                                   
                                   left   join npi_hco_mapping.lookup_npi_hco m
                                   on     na.npi = m.npi 
                                   
                                   left   join definitivehc.hospital_all_companies ac 
                                   on     m.defhc_id = ac.hospital_id
                            """).toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### 2B. Geographic Intersection

# COMMAND ----------

# Longitude & Latitude of the input facility 

df_input_coord = input_org_info.select('defhc_long', 'defhc_lat').toPandas().rename(columns={'defhc_long' :'longitude', 'defhc_lat': 'latitude'})

# COMMAND ----------

# convert input facility and three provider coordinate dfs to geopandas dfs and project to coordinates using meters

gdf_input_coord = get_coordinates(df_input_coord)

gdf_hco_coords = get_coordinates(df_hco_profile_coords)

gdf_hcp_coords = get_coordinates(df_hcp_profile_coords)

gdf_hco_npi_coords = get_coordinates(df_hco_npi_coords)

# COMMAND ----------

# Buffer and join to get intersection of each of the three providers to input facility

gdf_input_coord['geometry'] = gdf_input_coord['geometry'].buffer(1609.34*RADIUS) # miles to meters

df_nearby_hcos_id = get_intersection(base_gdf = gdf_input_coord,
                                 match_gdf = gdf_hco_coords,
                                 id_col = 'defhc_id',
                                 keep_coords='right',
                                 keep_cols=['primary'],
                                 keep_types=[IntegerType()])

df_nearby_hcps = get_intersection(base_gdf = gdf_input_coord,
                                 match_gdf = gdf_hcp_coords,
                                 id_col = 'NPI')

df_nearby_hcos_npi = get_intersection(base_gdf = gdf_input_coord,
                                      match_gdf = gdf_hco_npi_coords,
                                      id_col = 'NPI',
                                      keep_coords='right')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2C. HCO, HCP and NPI tables

# COMMAND ----------

# create temp views for hcos and hcps to join to external tables before saving

df_nearby_hcos_id.createOrReplaceTempView('df_nearby_hcos_id_vw')

df_nearby_hcps.createOrReplaceTempView('nearby_hcps_vw')

df_nearby_hcos_npi.createOrReplaceTempView('df_nearby_hcos_npi_vw')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 2Ci. Nearby HCOs (IDs)

# COMMAND ----------

# join nearby_hcos to d_profile to get firm and facility type,
# put asterisk around defhc_name if given facility (note currently not using this functionality to print out names, but including for posterity)

df_nearby_hcos_id2 = spark.sql(f"""
    select no.*
         , case when defhc_id = {DEFHC_ID} then concat('*', pf.ProfileName) 
                    else pf.ProfileName
                    end as defhc_name
         , pf.FirmTypeName
         , pf.FacilityTypeName
        
        , {assign_fac_types(alias='pf')}
                    
    from  df_nearby_hcos_id_vw no
    
    left join   martdim.d_profile pf 
    on          no.defhc_id = pf.DefinitiveId

""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{ProvRunInstance.fac_database}.nearby_hcos_id"

ProvRunInstance.create_final_output(df_nearby_hcos_id2, table=TBL_NAME)

# COMMAND ----------

# confirm unique by id for primary location

ProvRunInstance.test_distinct(sdf = hive_to_df(TBL_NAME).filter(F.col('primary')==1),
                              name = TBL_NAME,
                              cols = ['defhc_id']
                              )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 2Cii. Nearby HCPs

# COMMAND ----------

# join hcps_vw to nearby_hcps to add provider-level info and create flag for nearby providers
# save only nearby providers to permanent table

hcps_full = spark.sql(f"""
    select pv.npi        
        , pv.zip
        , pv.ProviderName
        , pv.net_defhc_name_hcp
        , pv.net_defhc_id_hcp
        , pv.network_flag_hcp
        , pv.defhc_id_primary
        , pv.defhc_name_primary
        , pv.affiliation_2cat
        , pv.affiliation_4cat
        
        , pv.PrimarySpecialty
        
        , pv.specialty_cat
        , pv.specialty_type
        , pv.include_pie
        
        , pv.npi_url
        
        , case when np.NPI is not null then 1 else 0 end as nearby
        
    from  hcps_base_vw pv

          left join  nearby_hcps_vw np 
          on   pv.NPI = np.NPI
""").checkpoint()

hcps_full.createOrReplaceTempView('hcps_full_vw')

# COMMAND ----------

# check distinct on full table

ProvRunInstance.test_distinct(sdf = hcps_full,
                              name = 'all_hcps_w_nearby',
                              cols = ['npi'],
                              to_subset = False
                              )

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{ProvRunInstance.fac_database}.nearby_hcps"

ProvRunInstance.create_final_output(hcps_full.filter(F.col('nearby')==1).drop('nearby'), table=TBL_NAME)

# COMMAND ----------

# check distinct on nearby only table

ProvRunInstance.test_distinct(sdf = hive_to_df(TBL_NAME),
                              name = TBL_NAME,
                              cols = ['npi']
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 2Ciii. Nearby HCOs (NPIs)

# COMMAND ----------

# join nearby hcos by NPI to temp view for NPIs with IDs and networks
# can use zip/lat/long for nearby only because only needed for nearby HCOs
# save only nearby to permanent table

hcos_npi_full = spark.sql(f"""
      select o.npi
            , hn.zip
            , hn.latitude
            , hn.longitude
            , o.defhc_id 
            , o.net_defhc_id
            , o.defhc_name
            , o.net_defhc_name
            
            , o.FirmTypeName
            , o.facility_type
            
            , o.network_flag
            
            , case when hn.NPI is not null then 1 else 0 end as nearby
            
    from   hcos_npi_base_vw o
    
           left join df_nearby_hcos_npi_vw hn

           on  o.npi = hn.npi
""")

hcos_npi_full.createOrReplaceTempView('hcos_npi_full_vw')

# COMMAND ----------

# check distinct on full table

ProvRunInstance.test_distinct(sdf = hcos_npi_full,
                              name = 'all_hcos_npi_w_nearby',
                              cols = ['npi'],
                              to_subset = False
                              )

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{ProvRunInstance.fac_database}.nearby_hcos_npi"

ProvRunInstance.create_final_output(hcos_npi_full.filter(F.col('nearby')==1).drop('nearby'), table=TBL_NAME)

# COMMAND ----------

# check distinct on nearby table

ProvRunInstance.test_distinct(sdf = hive_to_df(TBL_NAME),
                             name = TBL_NAME,
                             cols = ['npi']
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Claims Tables

# COMMAND ----------

# create variable with create age statement to use for both master claims and referrals

create_age_stmt = """ case when PatientBirthYearNum != 9999 then MxClaimYear - PatientBirthYearNum 
                           else null
                           end as patient_age
                  """

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3A. Master claims table

# COMMAND ----------

# inner join full claims to NEARBY HCO NPIs, and join to ALL HCP NPIs, keeping indicator for nearby HCP
# subset claims to given time period PLUS 90 days: the final perm table will only be given time frame,
# but the plus 90 days will be used to create inpatient stays in 3C

df_mxclaims_master = spark.sql(f"""
    select /*+ BROADCAST(pos) */
          mc.DHCClaimId
        , mc.ClaimTypeCd
        , mc.MxClaimYear
        , mc.MxClaimMonth
        , to_date(cast(mc.MxClaimDateKey as string), 'yyyyMMdd') as mxclaimdatekey
        , mc.PatientId 
        , mc.PlaceOfServiceCd
        , mc.PatientClaimZip3Cd as patient_zip3
        , mc.RenderingProviderNPI 
        , mc.BillingProviderNPI 
        , mc.FacilityNPI 
        , {create_age_stmt}
        
        , np.zip 
        , np.defhc_id
        , np.net_defhc_id
        , np.defhc_name
        , np.net_defhc_name
        , coalesce(np.network_flag, 'No Network') as network_flag
        , np.facility_type
        , np.FirmTypeName as defhc_fac_type
        
        , prov.PrimarySpecialty
        , prov.ProviderName
        , prov.specialty_cat
        , prov.specialty_type
        , prov.include_pie
        , prov.nearby as nearby_prov
                
        , prov.defhc_id_primary as provider_primary_affiliation_id
        , prov.affiliation_2cat
        , prov.affiliation_4cat
        
        , pos.pos_cat
        
        , prov.npi_url as rendering_npi_url
        
    from   MxMart.F_MxClaim mc 
    
           inner join (select * from hcos_npi_full_vw where nearby=1) np
           on         np.NPI = ifnull(mc.FacilityNPI, mc.BillingProviderNPI)
           
           left join  hcps_full_vw prov
           on         mc.RenderingProviderNPI = prov.NPI
           
           left join {DATABASE}.pos_category_assign pos
           on        mc.PlaceOfServiceCd = pos.PlaceOfServiceCd
           
    where  to_date(cast(mc.MxClaimDateKey as string), 'yyyyMMdd') between '{START_DATE}' and '{END_DATE_P90}' and
           mc.MxClaimYear >= 2016 and
           mc.MxClaimMonth between 1 and 12
           
           
""")

# COMMAND ----------

# if SUBSET_LT18 == 1, subset to age between 0 and 17, confirm correct ages kept

if SUBSET_LT18 == 1:    
    
    df_mxclaims_master = df_mxclaims_master.filter(F.col('patient_age').between(0, 17))

# COMMAND ----------

# subset perm claims table to given time frame, drop patient_age (not needed anymore) and defhc_fac_type (only used below for inpatient stays)

df_mxclaims_master_fnl = df_mxclaims_master.filter(F.col('mxclaimdatekey').between(START_DATE, END_DATE)) \
                                           .drop('patient_age', 'defhc_fac_type')

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{ProvRunInstance.fac_database}.{MX_CLMS_TBL}"

ProvRunInstance.create_final_output(df_mxclaims_master_fnl, table = TBL_NAME)

# COMMAND ----------

ProvRunInstance.test_distinct(sdf = hive_to_df(TBL_NAME),
                              name = TBL_NAME,
                              cols = ['DHCClaimId']
                              )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3B. PCP referrals claims table

# COMMAND ----------

# create temp view of stacked referrals (explit and explicit), and join to POS crosswalk to get pos_cat

referrals = spark.sql(f"""
        select a.*
             , pos.pos_cat as rend_pos_cat
             , pos2.pos_cat as ref_pos_cat
             , {create_age_stmt}
             
       from (
        
            select rend_claim_id
               ,   ref_NPI 
               ,   rend_NPI 
               ,   rend_claim_date
               ,   rend_claim_year as MxClaimYear
               ,   patient_id 
               ,   coalesce(rend_fac_npi, rend_bill_npi) as rend_fac_npi
               ,   coalesce(ref_fac_npi, ref_bill_npi) as ref_fac_npi
               ,   rend_pos
               ,   ref_pos
               ,   patient_birth_year as PatientBirthYearNum
            from   {DATABASE}.explicit_referrals 
            where  rend_claim_date between '{START_DATE}' and '{END_DATE}'

            union  distinct 

            select rend_claim_id
               ,   ref_NPI 
               ,   rend_NPI 
               ,   rend_claim_date
               ,   rend_claim_year as MxClaimYear
               ,   patient_id 
               ,   coalesce(rend_fac_npi, rend_bill_npi) as rend_fac_npi
               ,   coalesce(ref_fac_npi, ref_bill_npi) as ref_fac_npi
               ,   rend_pos
               ,   ref_pos
               ,   patient_birth_year as PatientBirthYearNum
            from   {DATABASE}.implicit_referrals_pcp_specialist
            where  rend_claim_date between '{START_DATE}' and '{END_DATE}'
        
        ) a
        
        left   join {DATABASE}.pos_category_assign pos
        on     a.rend_pos = pos.PlaceOfServiceCd
        
        left   join {DATABASE}.pos_category_assign pos2
        on     a.ref_pos = pos2.PlaceOfServiceCd
        
        
    """)

# COMMAND ----------

# if SUBSET_LT18 == 1, subset to age between 0 and 17

if SUBSET_LT18 == 1:    
    
    referrals = referrals.filter(F.col('patient_age').between(0, 17))

# COMMAND ----------

# checkpoint to truncate lineage to avoid out of memory error

referrals.checkpoint()

referrals.createOrReplaceTempView('referrals_vw')

# COMMAND ----------

# read in above referrals view and join TWICE to provider-level info (hcps_full_vw) for both PCP and spec

referrals1 = spark.sql(f"""
    select rend_claim_id
         , patient_id
         , rend_pos
         , rend_pos_cat
         , ref_pos
         , ref_pos_cat
        
        , ref_npi as npi_pcp
        , ref_fac_npi as npi_fac_pcp
        
        , ref.PrimarySpecialty as PrimarySpecialty_pcp
        , ref.specialty_cat as specialty_cat_pcp
        , ref.specialty_type as specialty_type_pcp
        , ref.ProviderName as name_pcp
        , ref.affiliation_2cat as affiliation_2cat_pcp
        , ref.affiliation_4cat as affiliation_4cat_pcp
        , ref.defhc_name_primary as affiliation_pcp
        , ref.zip as zip_pcp
        , ref.npi_url as npi_url_pcp
        , ref.net_defhc_name_hcp as raw_net_defhc_name_pcp
        , ref.net_defhc_id_hcp as raw_net_defhc_id_pcp
        , ref.network_flag_hcp as raw_network_flag_pcp
        
        , rend_npi as npi_spec
        , rend_fac_npi as npi_fac_spec
        
        , rend.PrimarySpecialty as PrimarySpecialty_spec
        , rend.specialty_cat as specialty_cat_spec
        , rend.specialty_type as specialty_type_spec
        , rend.ProviderName as name_spec
        , rend.affiliation_2cat as affiliation_2cat_spec
        , rend.affiliation_4cat as affiliation_4cat_spec
        , rend.defhc_name_primary as affiliation_spec
        , rend.zip as zip_spec
        , rend.npi_url as npi_url_spec
        , rend.net_defhc_name_hcp as raw_net_defhc_name_spec
        , rend.net_defhc_id_hcp as raw_net_defhc_id_spec
        , rend.network_flag_hcp as raw_network_flag_spec
        
        , ref.nearby as nearby_pcp
        , rend.nearby as nearby_spec
        
    from    referrals_vw a
    
            inner join hcps_full_vw ref
            on    a.ref_NPI = ref.npi

            inner join hcps_full_vw rend
            on   a.rend_NPI = rend.npi
            
   where ref.specialty_type = 'PCP' and 
         rend.specialty_type = 'Specialist'
           
""").checkpoint()

referrals1.createOrReplaceTempView('referrals1_vw')

# COMMAND ----------

# read in above view and again join TWICE to all HCOs to join on HCO-level info for both PCP and spec
# subset to nearby spec (rendering) facilities
# create network flags for ref and rend by coalescing HCO with HCP

referrals_fnl = spark.sql(f"""

    select *
    
    from (

        select a.*

            , ref.defhc_id as defhc_id_pcp 
            , coalesce(ref.net_defhc_id, raw_net_defhc_id_pcp) as net_defhc_id_pcp
            , ref.defhc_name as defhc_name_pcp
            , coalesce(ref.net_defhc_name, raw_net_defhc_name_pcp) as net_defhc_name_pcp
            , ref.facility_type as facility_type_pcp
            , ref.network_flag as network_flag_hco_pcp
            , coalesce(ref.network_flag, raw_network_flag_pcp, 'No Network') as network_flag_pcp

            , rend.defhc_id as defhc_id_spec
            , coalesce(rend.net_defhc_id, raw_net_defhc_id_spec) as net_defhc_id_spec
            , rend.defhc_name as defhc_name_spec
            , coalesce(rend.net_defhc_name, raw_net_defhc_name_spec) as net_defhc_name_spec
            , rend.facility_type as facility_type_spec
            , rend.network_flag as network_flag_hco_spec
            , coalesce(rend.network_flag, raw_network_flag_spec, 'No Network') as network_flag_spec

            , ref.nearby as nearby_fac_pcp
            , rend.nearby as nearby_fac_spec

        from referrals1_vw a 

            left   join hcos_npi_full_vw ref
            on     a.npi_fac_pcp = ref.npi

            inner   join hcos_npi_full_vw rend
            on     a.npi_fac_spec = rend.npi
        ) b
        
    where nearby_fac_spec=1
    
""")

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{ProvRunInstance.fac_database}.{PCP_REFS_TBL}"

ProvRunInstance.create_final_output(referrals_fnl, table=TBL_NAME)

# COMMAND ----------

# confirm distinct by rend_claim_id

ProvRunInstance.test_distinct(sdf = hive_to_df(TBL_NAME),
                              name = TBL_NAME,
                              cols = ['rend_claim_id']
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3C. Inpatient Stays

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 3Ci. Roll-up master claims to stay-level

# COMMAND ----------

# take master claims with 90 days post end date to create inpatient stays:
# keep needed cols and checkpoint the table, creating view to use for creation of stays and visits post-90

mxclaims_p90_sdf = df_mxclaims_master.select('DHCClaimId', 'MxClaimYear', 'MxClaimMonth', 'mxclaimdatekey', 'patientid', 'defhc_id',
                                             'defhc_name', 'network_flag', 'pos_cat', 'facility_type', 'defhc_fac_type') \
                                     .checkpoint()

mxclaims_p90_sdf.createOrReplaceTempView('mxclaims_p90_vw')

# COMMAND ----------

# use final master claims table (subset to given time frame) to identify all inpatient claims, 
# joining back to service lines to get min/max of service dates
# aggregate to patient/claim date/facility/provider-level, getting min and max of dates

inpat_claims_sdf = spark.sql(f"""

    select *
             -- create claim start and end dates based on EARLIEST and LATEST of all three combos
             
           , least(claim_date, min_service_to, min_service_from) as claim_start_date
           , greatest(claim_date, max_service_to, max_service_from) as claim_end_date
    
    from (
        select PatientId
            ,  mxclaimdatekey as claim_date
            ,  defhc_id
            ,  defhc_name
            ,  defhc_fac_type
            ,  network_flag

            ,  min_service_from
            ,  max_service_from
            
            ,  min_service_to
            ,  max_service_to

        from   mxclaims_p90_vw a 
        
               inner  join 
               
                (select MxClaimYear
                       ,MxClaimMonth
                       ,DHCClaimId
                       ,to_date(cast(min(ServiceFromDateKey) as string), 'yyyyMMdd') as min_service_from
                       ,to_date(cast(max(ServiceFromDateKey) as string), 'yyyyMMdd') as max_service_from
                       
                       ,to_date(cast(min(ServiceToDateKey) as string), 'yyyyMMdd') as min_service_to
                       ,to_date(cast(max(ServiceToDateKey) as string), 'yyyyMMdd') as max_service_to

                 from mxmart.f_mxservice
                 group by MxClaimYear
                         ,MxClaimMonth
                         ,DHCClaimId
                 ) b
                 
        on     a.MxClaimYear = b.MxClaimYear and 
               a.MxClaimMonth = b.MxClaimMonth and 
               a.DHCClaimId = b.DHCClaimId 

        where  a.pos_cat = 'Hospital Inpatient' and
               a.facility_type = 'Hospital' and 
               a.patientid is not null
    ) c

""")

# COMMAND ----------

# create windows to use below to identify new stays

lag_window = sdf_create_window(partition=['patientid', 'defhc_id'],
                               order = ['claim_start_date', 'claim_end_date'])
                              
sum_window = sdf_create_window(partition=['patientid', 'defhc_id'],
                               order = ['claim_start_date', 'claim_end_date'],
                               rows_between = 'unboundedPreceding')

stay_window = sdf_create_window(partition = ['patientid', 'defhc_id', 'stay_number'],
                               rows_between = 'unboundedBoth')

pat_window = sdf_create_window(partition = ['patientid', 'defhc_id'],
                              rows_between = 'unboundedBoth')

# COMMAND ----------

# lag dates from prior record and create an indicator for when to create a second stay (at least NEW_STAY_DAYS_CUTOFF between end and start of next)
# create stay start and end dates as min/max of start/end dates of individual claims for same stay_number (stay 1 begins at a value of 0)
# count total number of stays per patient/facility

inpat_claims_sdf2 = inpat_claims_sdf.withColumn('lag_end_date', F.lag('claim_end_date').over(lag_window)) \
                                    .withColumn('new_stay', F.when(F.datediff('claim_start_date', 'lag_end_date') > NEW_STAY_DAYS_CUTOFF, 1).otherwise(0)) \
                                    .withColumn('stay_number', F.sum('new_stay').over(sum_window)) \
                                    .withColumn('stay_start_date', F.min('claim_start_date').over(stay_window)) \
                                    .withColumn('stay_end_date', F.max('claim_end_date').over(stay_window)) \
                                    .withColumn('number_stays', F.lit(1) + F.max('stay_number').over(pat_window)) \
                                    .drop('claim_date', 'min_service_from', 'max_service_from', 'min_service_to', 'max_service_to')

# COMMAND ----------

# get distinct of stay-level columns to create stays
# create indicator for stays to include in baseline (stay_end_date on or before END_DATE) checkpoint table
# create unique stay_id (combo of patientid, defhc_id and stay_number)

inpat_stays_sdf = inpat_claims_sdf2.select('patientid', 'defhc_id', 'defhc_name', 'defhc_fac_type', 'network_flag', 'stay_number', 'stay_start_date', 'stay_end_date') \
                                   .distinct() \
                                   .withColumn('include_baseline', F.when(F.col('stay_end_date').between(START_DATE, END_DATE), 1).otherwise(0)) \
                                   .withColumn('stay_id', F.concat_ws('_', F.col('patientid'), F.col('defhc_id'), F.col('stay_number'))) \
                                   .checkpoint()
    
inpat_stays_sdf.createOrReplaceTempView('inpat_stays_vw')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 3Cii. Identify 90-day readmissions post-discharge

# COMMAND ----------

# to identify readmissions, must join all inpatient stays (subset to include_baseline==1 only) back to themselves by patientid,
# keeping any stays within 90 days of stay_end_date, and counting the UNIQUE stays by network_flag
# exclude joining given stay to itself
# exclude any records where ALL FOUR start/end dates are equal (ie both stays are one day each, do not know which occurred first)
# rename ID and name on initial stays to facility id/name to reflect what we have on facilities page (will need to link with those)

readmit_visits_sdf = spark.sql(f"""
        
        select a.patientid
               ,a.defhc_id as facility_id
               ,a.defhc_name as facility_name
                   
               ,b.network_flag
               ,b.defhc_id
               ,b.defhc_name
               ,b.defhc_fac_type
               
               ,a.stay_start_date
               ,a.stay_end_date
               ,a.stay_id
               
               ,datediff(b.stay_start_date, a.stay_end_date) as diff_days
               
               ,case when a.stay_start_date = a.stay_end_date and 
                          b.stay_start_date = b.stay_end_date and
                          datediff(b.stay_start_date, a.stay_end_date) = 0
                    then 1 
                    else 0
                    end as same_day_drop
               
        from (select * from inpat_stays_vw where include_baseline=1) a
             inner join
             inpat_stays_vw b
         
        on a.patientid = b.patientid and 
           a.stay_id != b.stay_id and
           datediff(b.stay_start_date, a.stay_end_date) between 0 and 90
    
""").filter(F.col('same_day_drop')==0)

# COMMAND ----------

# for dashboard: roll up by network_flag, getting count of distinct stay_id values

readmit_counts_sdf = readmit_visits_sdf.groupby('network_flag') \
                                       .agg(F.countDistinct(F.col('stay_id')).alias('count')) \
                                       .withColumn('place_of_service', F.lit('Hospital Inpatient'))

# COMMAND ----------

# for top 10 facilities: roll up by facility_id/facility_name (input facilities), and defhc_id/defhc_name/defhc_fac_type (post-discharge facilities)

readmit_fac_counts_sdf = readmit_visits_sdf.groupby('facility_id', 'facility_name', 'defhc_id', 'defhc_name', 'defhc_fac_type') \
                                           .agg(F.countDistinct(F.col('stay_id')).alias('count'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 3Ciii. Identify 90-day other visits post discharge

# COMMAND ----------

# to identify all other visits, must join all inpatient stays (subset to include_baseline==1 only) to master claims with 90 day runout,
# keeping any visits within 90 days of stay_end_date, and counting the UNIQUE visits by network_flag and pos
# filter master claims to non-inpatient POS with non-null patientid

other_visits_sdf = spark.sql(f"""
        
        select a.patientid
               ,a.defhc_id as facility_id
               ,a.defhc_name as facility_name
                   
               ,b.network_flag
               ,b.defhc_id
               ,b.defhc_name
               ,b.defhc_fac_type
               
               ,coalesce(b.pos_cat, 'Other') as place_of_service
               ,b.facility_type
               ,b.DHCClaimId
               
               ,a.stay_start_date
               ,a.stay_end_date
               
               ,b.mxclaimdatekey
               ,datediff(b.mxclaimdatekey, a.stay_end_date) as diff_days
               
        from (select * from inpat_stays_vw where include_baseline=1) a
        
             inner join
             
             (select * from mxclaims_p90_vw where pos_cat != 'Hospital Inpatient' and patientid is not null) b
             
        on a.patientid = b.patientid and
           datediff(b.mxclaimdatekey, a.stay_end_date) between 0 and 90
    
""")

# COMMAND ----------

# for dashboard: roll up by network_flag and place_of_service, getting count of distinct claim ID values

other_counts_sdf = other_visits_sdf.groupby('place_of_service', 'network_flag') \
                                   .agg(F.countDistinct(F.col('DHCClaimId')).alias('count')) 

# COMMAND ----------

# for top 10 facilities: roll up by facility_id/facility_name (input facilities), and defhc_id/defhc_name/defhc_fac_type (post-discharge facilities)

other_fac_counts_sdf = other_visits_sdf.groupby('facility_id', 'facility_name', 'defhc_id', 'defhc_name', 'defhc_fac_type') \
                                       .agg(F.countDistinct(F.col('DHCClaimId')).alias('count'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 3Civ. Output Dashboard counts

# COMMAND ----------

# for dashboard:
# stack readmit and other counts
# call create final output to join to base cols and add timestamp, and insert output for insert into table

inpat_counts_sdf = other_counts_sdf.unionByName(readmit_counts_sdf)

TBL_NAME = f"{ProvRunInstance.fac_database}.inpat90_dashboard"

ProvRunInstance.create_final_output(inpat_counts_sdf, table = TBL_NAME)

# COMMAND ----------

# confirm distinct by pos and network_flag

ProvRunInstance.test_distinct(sdf = hive_to_df(TBL_NAME),
                              name = TBL_NAME,
                              cols = ['place_of_service', 'network_flag']
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 3Cv. Output Facility top 10 counts

# COMMAND ----------

# for top 10 facilities:
# stack readmit and other counts, and sum counts across the two:
#   (the same destination facility could have inpatient and non-inpatient POS values, so could show up on both )

inpat_fac_counts_sdf = other_fac_counts_sdf.unionByName(readmit_fac_counts_sdf) \
                                           .groupby('facility_id', 'facility_name', 'defhc_id', 'defhc_name', 'defhc_fac_type') \
                                           .agg(F.sum(F.col('count')).alias('count'))

# COMMAND ----------

# call create final output to join to base cols and add timestamp, and insert output for insert into table

TBL_NAME = f"{ProvRunInstance.fac_database}.inpat90_facilities"

ProvRunInstance.create_final_output(inpat_fac_counts_sdf, table=TBL_NAME)

# COMMAND ----------

# confirm distinct by IDs/names

ProvRunInstance.test_distinct(sdf = hive_to_df(TBL_NAME),
                              name = TBL_NAME,
                              cols = ['facility_id', 'facility_name', 'defhc_id', 'defhc_name']
                             )

# COMMAND ----------

# finally, clear all checkpointed tables

rm_checkpoints(CHECKPOINT_DIR)

# COMMAND ----------

# create exit function to run either before QC checks or after based on RUN_QC

def exit():
    ProvRunInstance.exit_notebook()

# COMMAND ----------

if RUN_QC==0:
    exit()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. QC Frequencies

# COMMAND ----------

# HCO IDs: freq of firm type

sdf_frequency(hive_to_df(f"{ProvRunInstance.fac_database}.nearby_hcos_id"), ['FirmTypeName'])

# COMMAND ----------

# HCP: look at creation of affiliated and PCP flags

sdf_frequency(hcp_affs_net, ['primary_affiliation', 'secondary_affiliation', 'affiliation_2cat', 'affiliation_4cat'], order='cols')

sdf_frequency(hive_to_df(f"{ProvRunInstance.fac_database}.nearby_hcps"), ['specialty_type', 'PrimarySpecialty'], order='cols', maxobs=100)

# COMMAND ----------

# HCO NPIs: creation of network_flag, facility type from firm type

sdf_frequency(hive_to_df(f"{ProvRunInstance.fac_database}.nearby_hcos_npi"), ['network_flag', 'net_defhc_id', 'net_defhc_name'], order='cols', with_pct=True, maxobs=100)

sdf_frequency(hive_to_df(f"{ProvRunInstance.fac_database}.nearby_hcos_npi"), ['FirmTypeName', 'facility_type'], order='cols')

# COMMAND ----------

# CLAIMS: look at % null for joined on cols

sdf_claims = hive_to_df(f"{ProvRunInstance.fac_database}.{MX_CLMS_TBL}")

COLS = ['defhc_id', 'net_defhc_id']

for col in COLS:
    
    sdf_claims = add_null_indicator(sdf_claims, col)
    
    sdf_frequency(sdf_claims, [f"{col}_null"], with_pct=True)

# COMMAND ----------

# CLAIMS: crosstabs of pos_cat by pos, affiliated flag vs ID, nearby_prov

sdf_frequency(sdf_claims, ['pos_cat', 'PlaceOfServiceCd'], order='cols', with_pct=True, maxobs=100)

sdf_frequency(sdf_claims, ['nearby_prov'], with_pct=True)

# COMMAND ----------

# REFERRALS: crosstab of all indicators

sdf_frequency(hive_to_df(f"{ProvRunInstance.fac_database}.{PCP_REFS_TBL}"), ['nearby_pcp', 'nearby_spec', 'nearby_fac_pcp', 'nearby_fac_spec'], order='cols', with_pct=True)

# COMMAND ----------

exit()
