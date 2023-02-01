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
# MAGIC **NOTE**: DATABASE and TMP_DATABASE params below are value extracted from database widget, value passed to TMP_DATABASE() lambda func param, tbl var names specified in params
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {DATABASE}.hcp_specialty_assignment
# MAGIC   - {DATABASE}.pos_category_assign
# MAGIC   - definitivehc.hospital_all_companies
# MAGIC   - npi_hco_mapping.dhc_pg_location_addresses
# MAGIC   - definitivehc.hospital_physician_compare_physicians
# MAGIC   - npi_hco_mapping.npi_registry_addresses
# MAGIC   - hcp_affiliations.physician_org_affiliations
# MAGIC   - MxMart.F_MxClaim_v2
# MAGIC   - MartDim.D_Organization
# MAGIC   - MartDim.D_Profile
# MAGIC   - MartDim.D_Provider
# MAGIC   - ds_payer_mastering.mx_claims_imputed_payers
# MAGIC   - ds_payer_mastering.encoded_payer_id_mapping
# MAGIC   - {DATABASE}.explicit_referrals
# MAGIC   - {DATABASE}.implicit_referrals_pcp_specialist
# MAGIC   
# MAGIC **Outputs**:
# MAGIC   - {TMP_DATABASE}.input_org_info
# MAGIC   - {TMP_DATABASE}.nearby_hcos
# MAGIC   - {TMP_DATABASE}.nearby_hcps
# MAGIC   - {TMP_DATABASE}.nearby_npis
# MAGIC   - {TMP_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {TMP_DATABASE}.{PCP_REFS_TBL}

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

# get widget values

RUN_VALUES = get_widgets()

DEFHC_ID, RADIUS, START_DATE, END_DATE, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'DATABASE', 'RUN_QC'])

TMP_DATABASE = GET_TMP_DATABASE(DATABASE)

# create dictionary of counts to fill in for each perm table and return on pass

COUNTS_DICT = {}

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
        ,  network_id as input_network
        ,  firm_type
        ,  trim(concat(ifnull(hq_address, ''), ' ', ifnull(hq_address1, ''))) as address 
        ,  hq_city as defhc_city
        ,  hq_state as defhc_state
        ,  hq_zip_code as defhc_zip
        ,  hq_latitude as defhc_lat
        ,  hq_longitude as defhc_long
    from   definitivehc.hospital_all_companies 
    where  hospital_id = {DEFHC_ID}""")

pyspark_to_hive(input_org_info,
               f"{TMP_DATABASE}.input_org_info")

# COMMAND ----------

# print record

hive_sample(f"{TMP_DATABASE}.input_org_info")

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
    exit_notebook(f"Input Definitive ID {DEFHC_ID} not found in database")

if INPUT_NETWORK is None:
    exit_notebook(f"Input Definitive ID {DEFHC_ID} is missing parent network")
    
print(f"Input network: {INPUT_NETWORK}")
print(f"Input firm type: {FIRM_TYPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1B. Create temp view of all HCPs with IDs/affiliations/specialty

# COMMAND ----------

# create temp view with top given firm type affiliation for every provider

prim_aff = spark.sql(f"""
    select physician_npi
           ,defhc_id as defhc_id_primary
           ,hospital_name as defhc_name_primary
           ,score
           ,score_bucket
           ,row_number() over (partition by physician_npi 
                               order by score_bucket desc, score desc)
                         as rn

            from   hcp_affiliations.physician_org_affiliations
            where  include_flag = 1 and
                   current_flag = 1 and 
                   firm_type='{FIRM_TYPE}'
       """)

prim_aff.filter(F.col('rn')==1).createOrReplaceTempView('prim_aff_vw')

# COMMAND ----------

# take martdim.d_provider as source of truth for ALL individual NPIs, and join on other NPI-info including affiliations created above

hcps = spark.sql(f"""

    select cast(pv.NPI as int) as npi
          ,pv.PrimarySpecialty
          ,pv.ProviderName
          
          ,sp.specialty_cat
          ,sp.specialty_type
          ,sp.include_pie
          
          ,aff.defhc_id_primary
          ,aff.defhc_name_primary
          
          , {affiliated_flag('aff.defhc_id_primary', DEFHC_ID)}
          
          ,cp.hq_zip_code as zip
          ,concat("{PHYS_LINK}", pv.npi) as npi_url
          
    from martdim.d_provider pv

          left join   prim_aff_vw aff
          on          pv.NPI = aff.physician_npi

          left join  {DATABASE}.hcp_specialty_assignment sp
          on         pv.PrimarySpecialty = sp.specialty_name
          
          left join  definitivehc.hospital_physician_compare_physicians cp
          on         pv.NPI = cp.npi

""")

hcps.createOrReplaceTempView('hcps_base_vw')

# COMMAND ----------

# confirm unique by npi

test_distinct(sdf = hcps,
              name = 'all_hcps',
              cols = ['npi']
             )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1C. Create temp view of all HCO NPIs with IDs/networks

# COMMAND ----------

"""
  join all active org NPIs to d_profile to get defhc_id and network def_hcid
  to determine network to assign, we will use a two-step process:
      -- if EITHER NetworkDefinitiveId OR NetworkParentDefinitiveID (network or parent network) == input network, assign input network
      -- otherwise, assign network as coalesce(NetworkParentDefinitiveID, NetworkDefinitiveId), to prioritize parent if assigned, otherwise use network

"""

hcos_npi = spark.sql(f"""

    select *
          , {network_flag('net_defhc_id', INPUT_NETWORK)}
          
    from (

        select cast(o.NPI as int) as NPI
             , p.DefinitiveId as defhc_id 
             , p.NetworkDefinitiveId
             , p.ProfileName as defhc_name
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
                    
            , case when p.NetworkDefinitiveId = {INPUT_NETWORK}
                   then p.NetworkName
                   
                   when p.NetworkParentDefinitiveID = {INPUT_NETWORK}
                   then p.NetworkParentName
                   
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

test_distinct(sdf = hcos_npi,
              name = 'all_hcos_npi',
              cols = ['npi']
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

# join nearby_hcos to d_profile to get firm and facility type

df_nearby_hcos_id = spark.sql(f"""
    select no.*
        ,  pf.ProfileName as defhc_name
        ,  pf.FirmTypeName
        ,  pf.FacilityTypeName
        
        , {assign_fac_types(alias='pf')}
                    
    from  df_nearby_hcos_id_vw no
    
    left join   martdim.d_profile pf 
    on          no.defhc_id = pf.DefinitiveId

""")

# COMMAND ----------

# save to temp directory, put counts into dictionary, and print sample of records

TBL = f"{TMP_DATABASE}.nearby_hcos_id"

pyspark_to_hive(df_nearby_hcos_id,
               TBL)

COUNTS_DICT[TBL] = hive_tbl_count(TBL)

hive_sample(TBL)

# COMMAND ----------

# confirm unique by id for primary location

test_distinct(sdf = hive_to_df(f"{TMP_DATABASE}.nearby_hcos_id").filter(F.col('primary')==1),
              name = f"{TMP_DATABASE}.nearby_hcos_id",
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
        , pv.defhc_id_primary
        , pv.defhc_name_primary
        , pv.affiliated_flag
        
        , pv.PrimarySpecialty
        
        , pv.specialty_cat
        , pv.specialty_type
        , pv.include_pie
        
        , pv.npi_url
        
        , case when np.NPI is not null then 1 else 0 end as nearby
        
    from  hcps_base_vw pv

          left join  nearby_hcps_vw np 
          on   pv.NPI = np.NPI
""")

hcps_full.createOrReplaceTempView('hcps_full_vw')

# COMMAND ----------

# check distinct on full table

test_distinct(sdf = hcps_full,
              name = 'all_hcps_w_nearby',
              cols = ['npi']
             )

# COMMAND ----------

# save to temp directory (filtering to nearby==1), put counts into dictionary, and print sample of records

TBL = f"{TMP_DATABASE}.nearby_hcps"

pyspark_to_hive(hcps_full.filter(F.col('nearby')==1).drop('nearby'),
               TBL)

COUNTS_DICT[TBL] = hive_tbl_count(TBL)

hive_sample(TBL)

# COMMAND ----------

# check distinct on nearby only table

test_distinct(sdf = hive_to_df(f"{TMP_DATABASE}.nearby_hcps"),
              name = f"{TMP_DATABASE}.nearby_hcps",
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

test_distinct(sdf = hcos_npi_full,
              name = 'all_hcos_npi_w_nearby',
              cols = ['npi']
             )

# COMMAND ----------

# save to temp directory (filtering to nearby==1), put counts into dictionary, and print sample of records

TBL = f"{TMP_DATABASE}.nearby_hcos_npi"

pyspark_to_hive(hcos_npi_full.filter(F.col('nearby')==1).drop('nearby'),
               TBL)

COUNTS_DICT[TBL] = hive_tbl_count(TBL)

hive_sample(TBL)

# COMMAND ----------

# check distinct on nearby table

test_distinct(sdf = hive_to_df(f"{TMP_DATABASE}.nearby_hcos_npi"),
              name = f"{TMP_DATABASE}.nearby_hcos_npi",
              cols = ['npi']
             )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Claims Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3A. Master claims table

# COMMAND ----------

df_mxclaims_master = spark.sql(f"""
    select /*+ BROADCAST(pos) */
           mc.DHCClaimId
        ,  mc.ClaimTypeCd
        ,  to_date(cast(mc.MxClaimDateKey as string), 'yyyyMMdd') as mxclaimdatekey
        ,  mc.PatientId 
        ,  mc.PlaceOfServiceCd
        ,  mc.PatientClaimZip3Cd as patient_zip3
        ,  mc.RenderingProviderNPI 
        ,  mc.BillingProviderNPI 
        ,  mc.FacilityNPI 
        
        ,  np.zip 
        ,  np.defhc_id 
        ,  np.net_defhc_id
        ,  np.defhc_name
        ,  np.net_defhc_name
        ,  np.network_flag
        ,  np.facility_type
        
        , prov.PrimarySpecialty
        , prov.ProviderName
        , prov.specialty_cat
        , prov.specialty_type
        , prov.include_pie
        , prov.nearby
                
        , prov.defhc_id_primary as provider_primary_affiliation_id
        , {affiliated_flag('prov.defhc_id_primary', DEFHC_ID)}
        
        , pos.pos_cat
        
        , prov.npi_url as rendering_npi_url
        
    from   MxMart.F_MxClaim_v2 mc 
           inner join
           {TMP_DATABASE}.nearby_hcos_npi np
           on np.NPI = ifnull(mc.FacilityNPI, mc.BillingProviderNPI)
           
           left join  hcps_full_vw prov
           on         mc.RenderingProviderNPI = prov.NPI
           
           left   join {DATABASE}.pos_category_assign pos
           on     mc.PlaceOfServiceCd = pos.PlaceOfServiceCd
           
    where  to_date(cast(mc.MxClaimDateKey as string), 'yyyyMMdd') between '{START_DATE}' and '{END_DATE}' and
           mc.MxClaimYear >= 2016 and
           mc.MxClaimMonth between 1 and 12
           
""")

# COMMAND ----------

# save to temp directory, put counts into dictionary, and print sample of records

TBL = f"{TMP_DATABASE}.{MX_CLMS_TBL}"

pyspark_to_hive(df_mxclaims_master,
               TBL)

COUNTS_DICT[TBL] = hive_tbl_count(TBL)

hive_sample(TBL)

# COMMAND ----------

test_distinct(sdf = hive_to_df(f"{TMP_DATABASE}.{MX_CLMS_TBL}"),
              name = f"{TMP_DATABASE}.{MX_CLMS_TBL}",
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
               
       from (
        
            select rend_claim_id
               ,   ref_NPI 
               ,   rend_NPI 
               ,   rend_claim_date
               ,   patient_id 
               ,   coalesce(rend_fac_npi, rend_bill_npi) as rend_fac_npi
               ,   coalesce(ref_fac_npi, ref_bill_npi) as ref_fac_npi
               ,   rend_pos
            from   {DATABASE}.explicit_referrals 
            where  rend_claim_date between '{START_DATE}' and '{END_DATE}'

            union  distinct 

            select rend_claim_id
               ,   ref_NPI 
               ,   rend_NPI 
               ,   rend_claim_date
               ,   patient_id 
               ,   coalesce(rend_fac_npi, rend_bill_npi) as rend_fac_npi
               ,   coalesce(ref_fac_npi, ref_bill_npi) as ref_fac_npi
               ,   rend_pos
            from   {DATABASE}.implicit_referrals_pcp_specialist
            where  rend_claim_date between '{START_DATE}' and '{END_DATE}'
        
        ) a
        
        left   join {DATABASE}.pos_category_assign pos
        on     a.rend_pos = pos.PlaceOfServiceCd

        
    """)

referrals.createOrReplaceTempView('referrals_vw')

# COMMAND ----------

# read in above referrals view and join TWICE to provider-level info (hcps_full_vw) for both PCP and spec

referrals1 = spark.sql(f"""
    select rend_claim_id
         , patient_id
         , rend_pos
         , rend_pos_cat
        
        , ref_npi as npi_pcp
        , ref_fac_npi as npi_fac_pcp
        
        , ref.PrimarySpecialty as PrimarySpecialty_pcp
        , ref.specialty_cat as specialty_cat_pcp
        , ref.specialty_type as specialty_type_pcp
        , ref.ProviderName as name_pcp
        , ref.affiliated_flag as affiliated_flag_pcp
        , ref.defhc_name_primary as affiliation_pcp
        , ref.zip as zip_pcp
        , ref.npi_url as npi_url_pcp
        
        , rend_npi as npi_spec
        , rend_fac_npi as npi_fac_spec
        
        , rend.PrimarySpecialty as PrimarySpecialty_spec
        , rend.specialty_cat as specialty_cat_spec
        , rend.specialty_type as specialty_type_spec
        , rend.ProviderName as name_spec
        , rend.affiliated_flag as affiliated_flag_spec
        , rend.defhc_name_primary as affiliation_spec
        , rend.zip as zip_spec
        , rend.npi_url as npi_url_spec
        
        , ref.nearby as nearby_pcp
        , rend.nearby as nearby_spec
        
    from    referrals_vw a
    
            inner join hcps_full_vw ref
            on    a.ref_NPI = ref.npi

            inner join hcps_full_vw rend
            on   a.rend_NPI = rend.npi
            
   where ref.specialty_type = 'PCP' and 
         rend.specialty_type = 'Specialist'
           
""")

referrals1.createOrReplaceTempView('referrals1_vw')

# COMMAND ----------

# read in above view and again join TWICE to all HCOs to join on HCO-level info for both PCP and spec,
# and subsetting to either BOTH providers nearby, or nearby PCP or spec facility

referrals_fnl = spark.sql(f"""

    select *
    
    from (

        select a.*

            , ref.defhc_id as defhc_id_pcp 
            , ref.net_defhc_id as net_defhc_id_pcp
            , ref.defhc_name as defhc_name_pcp
            , ref.net_defhc_name as net_defhc_name_pcp
            , ref.facility_type as facility_type_pcp
            , ref.network_flag as network_flag_pcp

            , rend.defhc_id as defhc_id_spec
            , rend.net_defhc_id as net_defhc_id_spec
            , rend.defhc_name as defhc_name_spec
            , rend.net_defhc_name as net_defhc_name_spec
            , rend.facility_type as facility_type_spec
            , rend.network_flag as network_flag_spec

            , ref.nearby as nearby_fac_pcp
            , rend.nearby as nearby_fac_spec

        from referrals1_vw a 

            left   join hcos_npi_full_vw ref
            on     a.npi_fac_pcp = ref.npi

            left   join hcos_npi_full_vw rend
            on     a.npi_fac_spec = rend.npi
        ) b
        
    where (nearby_pcp=1 and nearby_spec=1) or nearby_fac_pcp=1 or nearby_fac_spec=1
    
""")

# COMMAND ----------

# save to temp directory, put counts into dictionary, and print sample of records

TBL = f"{TMP_DATABASE}.{PCP_REFS_TBL}"

pyspark_to_hive(referrals_fnl,
               TBL)

COUNTS_DICT[TBL] = hive_tbl_count(TBL)

hive_sample(TBL)

# COMMAND ----------

# confirm distinct by rend_claim_id

test_distinct(sdf = hive_to_df(f"{TMP_DATABASE}.{PCP_REFS_TBL}"),
              name = f"{TMP_DATABASE}.{PCP_REFS_TBL}",
              cols = ['rend_claim_id']
             )

# COMMAND ----------

# create exit function to run either before QC checks or after based on RUN_QC

def exit():
    exit_notebook(COUNTS_DICT,
                  fail=False)

# COMMAND ----------

if RUN_QC==0:
    exit()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. QC Frequencies

# COMMAND ----------

# HCO IDs: freq of firm type

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.nearby_hcos_id"), ['FirmTypeName'])

# COMMAND ----------

# HCP: look at creation of affiliated and PCP flags

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.nearby_hcps"), ['affiliated_flag', 'defhc_id_primary'])

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.nearby_hcps"), ['specialty_type', 'PrimarySpecialty'], order='cols', maxobs=100)

# COMMAND ----------

# HCO NPIs: creation of network_flag, facility type from firm type

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.nearby_hcos_npi"), ['network_flag', 'net_defhc_id', 'net_defhc_name'], order='cols', with_pct=True, maxobs=100)

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.nearby_hcos_npi"), ['FirmTypeName', 'facility_type'], order='cols')

# COMMAND ----------

# CLAIMS: look at % null for joined on cols

sdf_claims = hive_to_df(f"{TMP_DATABASE}.{MX_CLMS_TBL}")

COLS = ['defhc_id', 'net_defhc_id', 'payer_id', 'payer_name']

for col in COLS:
    
    sdf_claims = add_null_indicator(sdf_claims, col)
    
    sdf_frequency(sdf_claims, [f"{col}_null"], with_pct=True)

# COMMAND ----------

# CLAIMS: crosstabs of pos_cat by pos, affiliated flag vs ID

sdf_frequency(sdf_claims, ['pos_cat', 'PlaceOfServiceCd'], order='cols', with_pct=True, maxobs=100)

sdf_frequency(sdf_claims, ['affiliated_flag', 'provider_primary_affiliation_id'])

# COMMAND ----------

# REFERRALS: crosstab of all indicators

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.{PCP_REFS_TBL}"), ['nearby_pcp', 'nearby_spec', 'nearby_fac_pcp', 'nearby_fac_spec'], order='cols', with_pct=True)

# COMMAND ----------

exit()
