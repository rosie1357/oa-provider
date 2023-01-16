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
# MAGIC   - MxMart.F_MxClaim_v2
# MAGIC   - MartDim.D_Organization
# MAGIC   - MartDim.D_Profile
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

#TODO: figure out how to write all var names ONCE and convert list to variables/vice-versa

RUN_VALUES = get_widgets()

DEFHC_ID, RADIUS, START_DATE, END_DATE, DATABASE, RUN_QC = return_widget_values(RUN_VALUES, ['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'DATABASE', 'RUN_QC'])

TMP_DATABASE = GET_TMP_DATABASE(DATABASE)

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
# MAGIC #### 1B. Create temp view of primary affiliations

# COMMAND ----------

# create temp view with top given firm type affiliation for every provider
# will use for nearby HCPs and affiliated providers 

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
                                from   definitivehc.hospital_all_companies 
                                
                                union  distinct 
                                
                                select defhc_id
                                    ,  hq_longitude
                                    ,  hq_latitude 
                                    ,  facility_zip
                                from   npi_hco_mapping.dhc_pg_location_addresses
                                """).toPandas()

# Latitude & Longitude by TYPE 1 NPI (only those tracked as physicians)
# This data is complete, i.e. every physician has a longitude and latitude 

df_hcp_profile_coords = spark.sql(f"""
                                select cast(NPI as int) as NPI
                                    ,  longitude
                                    ,  latitude
                                    ,  hq_zip_code as zip
                                from   definitivehc.hospital_physician_compare_physicians
                                """).toPandas()

# Latitude & Longitude by TYPE 2 NPI (from NPI registry)
# This data may not be complete, as it relies on the geocoding processes from DB engineering and Data Science 
# Some addresses fail geocoding 
# Default to the longitude and latitude of the main location from the defhc profile if not available at the NPI level 
# It is preferable to use the geo of the NPI when using claims data especially for profiles that have multiple locations (PGs)

df_hco_npi_coords = spark.sql(f"""
                            select cast(na.NPI as int) as NPI
                                ,  ifnull(geo.longitude, ac.hq_longitude) as longitude
                                ,  ifnull(geo.latitude, ac.hq_latitude) as latitude 
                                ,  zip_code as zip 
                                ,  m.defhc_id
                                
                            from   npi_hco_mapping.npi_registry_addresses na 
                                   left   join npi_hco_mapping.ds_geocoded_addresses geo
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
                                 id_col = 'defhc_id')

df_nearby_hcps = get_intersection(base_gdf = gdf_input_coord,
                                 match_gdf = gdf_hcp_coords,
                                 id_col = 'NPI')

df_nearby_hcos_npi = get_intersection(base_gdf = gdf_input_coord,
                                      match_gdf = gdf_hco_npi_coords,
                                      id_col = 'NPI')

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

# join nearby_hcos to d_profile to get firm and facility type, and save to temp directory

df_nearby_hcos_id = spark.sql(f"""
    select no.*
        ,  pf.FirmTypeName
        ,  pf.FacilityTypeName
        
    from  df_nearby_hcos_id_vw no
    
    left join   martdim.d_profile pf 
    on          no.defhc_id = pf.DefinitiveId

""")

pyspark_to_hive(df_nearby_hcos_id,
               f"{TMP_DATABASE}.nearby_hcos_id")

# COMMAND ----------

# print sample of records

hive_sample(f"{TMP_DATABASE}.nearby_hcos_id")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 2Cii. Nearby HCPs

# COMMAND ----------

# join nearby_hcps_vw to d_provider to get provider info, and primary affiliations to get primary hospital affiliation
# join to pcp/specialist table to get specialist info
# create affiliated flag based on primary id flag
# create text link to physician page
# link to physician page

df_nearby_hcps_spec = spark.sql(f"""
    select np.*
        ,  pv.ProviderName
        ,  pa.defhc_id_primary
        ,  pa.defhc_name_primary
        , {affiliated_flag('defhc_id_primary', DEFHC_ID)}
        
        ,  pv.PrimarySpecialty
        
        , sp.specialty_cat
        , sp.specialty_type
        
        , concat("{PHYS_LINK}", np.npi) as npi_url
        
    from  nearby_hcps_vw np

    left join martdim.d_provider pv 
    on          np.NPI = pv.NPI
    
    left join   prim_aff_vw pa
    on          np.NPI = pa.physician_npi
    
    left join  {DATABASE}.hcp_specialty_assignment sp
    on         pv.PrimarySpecialty = sp.specialty_name
""")

pyspark_to_hive(df_nearby_hcps_spec,
               f"{TMP_DATABASE}.nearby_hcps")

# COMMAND ----------

# print sample of records

hive_sample(f"{TMP_DATABASE}.nearby_hcps")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 2Ciii. Nearby HCOs (NPIs)

# COMMAND ----------

# join nearby hcos by NPI to the following:
#    D_Organization to get defhc_id
#    D_Profile to get network IDs

df_nearby_hcos_npi = spark.sql(f"""
    select hn.*
            ,  p.DefinitiveId as defhc_id 
            ,  p.NetworkDefinitiveId as net_defhc_id
            ,  p.ProfileName as defhc_name
            ,  netp.ProfileName as net_defhc_name
            
            , {network_flag('p.NetworkDefinitiveId', INPUT_NETWORK)}

    
    from   df_nearby_hcos_npi_vw hn

           left   join MartDim.D_Organization o 
           on     hn.npi = o.npi and
                  o.ActiveRecordInd is True 
                  
           left   join MartDim.D_Profile p 
           on     o.DefinitiveId = p.DefinitiveId
           
           left   join MartDim.D_Profile netp
           on     p.NetworkDefinitiveId = netp.DefinitiveId
""")

pyspark_to_hive(df_nearby_hcos_npi,
               f"{TMP_DATABASE}.nearby_hcos_npi")

# COMMAND ----------

# print sample of records

hive_sample(f"{TMP_DATABASE}.nearby_hcos_npi")

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
    select /*+ BROADCAST(pos), BROADCAST(sp) */
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
        
        ,  pm.defhc_id as payer_id 
        ,  pm.defhc_name as payer_name
        
        , prov.PrimarySpecialty
        , prov.ProviderName
        , sp.specialty_cat
        , sp.specialty_type
        , sp.include_pie
                
        , aff.defhc_id_primary as provider_primary_affiliation_id
        , {affiliated_flag('aff.defhc_id_primary', DEFHC_ID)}
        
        , pos.pos_cat
        
        , concat("{PHYS_LINK}", RenderingProviderNPI) as rendering_npi_url
        
    from   MxMart.F_MxClaim_v2 mc 
           inner join
           {TMP_DATABASE}.nearby_hcos_npi np
           on np.NPI = ifnull(mc.FacilityNPI, mc.BillingProviderNPI)
    
           
           left   join ds_payer_mastering.mx_claims_imputed_payers pay 
           on     mc.DHCClaimId = pay.DHCClaimId and
                  mc.MxClaimYear = pay.MxClaimYear and
                  mc.MxClaimMonth = pay.MxClaimMonth
                  
           left   join ds_payer_mastering.encoded_payer_id_mapping pm 
           on     pay.dh_encoded_id = pm.dh_encoded_id 
           
           left join  MartDim.D_Provider prov
           on         mc.RenderingProviderNPI = prov.NPI
           
           left join  prim_aff_vw aff
           on         mc.RenderingProviderNPI = aff.physician_npi
           
           left   join {DATABASE}.pos_category_assign pos
           on     mc.PlaceOfServiceCd = pos.PlaceOfServiceCd
           
           left join  {DATABASE}.hcp_specialty_assignment sp
           on         prov.PrimarySpecialty = sp.specialty_name
           
    where  to_date(cast(mc.MxClaimDateKey as string), 'yyyyMMdd') between '{START_DATE}' and '{END_DATE}' and
           mc.MxClaimYear >= 2016 and
           mc.MxClaimMonth between 1 and 12
           
""")

# COMMAND ----------

# save to temp database

pyspark_to_hive(df_mxclaims_master,
               f"{TMP_DATABASE}.{MX_CLMS_TBL}")

# COMMAND ----------

# print sample of records

hive_sample(f"{TMP_DATABASE}.{MX_CLMS_TBL}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3B. PCP referrals claims table

# COMMAND ----------

# create temp view of stacked referrals (explit and explicit), create network flags for both rendering AND referring providers

referrals = spark.sql(f"""
        select a.*
               ,pos.pos_cat as rend_pos_cat
               ,{network_flag('rend_network_id', INPUT_NETWORK, suffix='_spec')}
               ,{network_flag('ref_network_id', INPUT_NETWORK, suffix='_pcp')}
                     
       from (
        
            select ref_NPI 
               ,   rend_NPI 
               ,   rend_claim_date
               ,   patient_id 
               ,   rend_fac_npi
               ,   rend_pos
               ,   coalesce(rend_fac_network_id, rend_bill_network_id, rendering_primary_network_id) as rend_network_id
               ,   coalesce(ref_fac_network_id, ref_bill_network_id, referring_primary_network_id) as ref_network_id
            from   {DATABASE}.explicit_referrals 
            where  rend_claim_date between '{START_DATE}' and '{END_DATE}'

            union  distinct 

            select ref_NPI 
               ,   rend_NPI 
               ,   rend_claim_date
               ,   patient_id 
               ,   rend_fac_npi
               ,   rend_pos
               ,   coalesce(rend_fac_network_id, rend_bill_network_id, rendering_primary_network_id) as rend_network_id
               ,   coalesce(ref_fac_network_id, ref_bill_network_id, referring_primary_network_id) as ref_network_id
            from   {DATABASE}.implicit_referrals_pcp_specialist
            where  rend_claim_date between '{START_DATE}' and '{END_DATE}'
        
        ) a
        
    left   join {DATABASE}.pos_category_assign pos
    on     a.rend_pos = pos.PlaceOfServiceCd
        
    """)

referrals.createOrReplaceTempView('referrals_vw')

# COMMAND ----------

# create referrals by joining to nearby hcps TWICE:
#  want to subset to both rend AND ref providers nearby,
#  and where referring provider is PCP and rendering provider is specialist


df_referrals = spark.sql(f"""
    select patient_id
        , rend_fac_npi
        , rend_pos
        , rend_pos_cat
        
        , ref_npi as npi_pcp
        , ref_network_id as network_id_pcp
        , network_flag_pcp
        
        , ref.PrimarySpecialty as PrimarySpecialty_pcp
        , ref.specialty_cat as specialty_cat_pcp
        , ref.specialty_type as specialty_type_pcp
        , ref.ProviderName as name_pcp
        , ref.affiliated_flag as affiliated_flag_pcp
        , ref.defhc_name_primary as affiliation_pcp
        , ref.npi_url as npi_url_pcp
        
        , rend_npi as npi_spec
        , rend_network_id as network_id_spec
        , network_flag_spec
        
        , rend.PrimarySpecialty as PrimarySpecialty_spec
        , rend.specialty_cat as specialty_cat_spec
        , rend.specialty_type as specialty_type_spec
        , rend.ProviderName as name_spec
        , rend.affiliated_flag as affiliated_flag_spec
        , rend.defhc_name_primary as affiliation_spec
        , rend.npi_url as npi_url_spec
        
    from   referrals_vw a

    inner join {TMP_DATABASE}.nearby_hcps ref
    on a.ref_npi = ref.npi
     
    inner join {TMP_DATABASE}.nearby_hcps rend
    on a.rend_npi = rend.npi
    
    where ref.specialty_type = 'PCP' and 
          rend.specialty_type = 'Specialist'
           
""")

# COMMAND ----------

# save to temp database

pyspark_to_hive(df_referrals,
               f"{TMP_DATABASE}.{PCP_REFS_TBL}", overwrite_schema='true')

# COMMAND ----------

# print sample of records

hive_sample(f"{TMP_DATABASE}.{PCP_REFS_TBL}")

# COMMAND ----------

# create exit function to run either before QC checks or after based on RUN_QC

def exit():
    exit_notebook(f"All provider and claims tables created for {DEFHC_ID}!",
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

# HCO NPIs: creation of network_flag

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.nearby_hcos_npi"), ['network_flag', 'net_defhc_id', 'net_defhc_name'], order='cols', with_pct=True, maxobs=100)

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

# REFERRALS: crosstab of network by network flag

sdf_frequency(hive_to_df(f"{TMP_DATABASE}.{PCP_REFS_TBL}"), ['network_id_spec', 'network_flag_spec'], with_pct=True, maxobs=100)

# COMMAND ----------

exit()
