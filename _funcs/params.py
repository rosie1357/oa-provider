REF_S3_BUCKET = 's3://dhc-datascience-prod/ds_provider/reference_data'
S3_BUCKET = 'dhc-ize'
S3_KEY = 'oppurtunity-assesment/clientdata'

# link to physician page

PHYS_LINK = "https://www.defhc.com/physicians/"

# variables to hold names of base/output tables

MX_CLMS_TBL = 'mxclaims_master'
PCP_REFS_TBL = 'pcp_referrals'

# list of ALL main tables created in 001_create_tables

ALL_TABLES = ['input_org_info',
              'nearby_hcos_id',
              'nearby_hcps',
              'nearby_hcos_npi',
              'inpat90_dashboard',
              'inpat90_facilities',
              MX_CLMS_TBL,
              PCP_REFS_TBL
             ]

COUNTS_TBL = 'record_counts'
STATUS_TBL = 'run_status'


GET_FAC_DATABASE = lambda db, id: f"{db}_{id}"

NEW_STAY_DAYS_CUTOFF = 7

# set checkpoint dir

CHECKPOINT_DIR = '/FileStore/checkpoints/provider'

# create string to use in multiple charts to subset to hospital/ASC/HOPD claims

HOSP_ASC_HOPD_SUBSET = """
    (
     (pos_cat='ASC & HOPD' and facility_type in ('Ambulatory Surgery Center', 'Hospital')) or
     (pos_cat='Hospital Inpatient' and facility_type='Hospital') 
    )

"""

def network_flag(network_col, network_value, suffix=''):
    """
    Function network_flag() to create col network_flag based on network col name and literal value
    params:
        network_col str: name of column with network values to match against
        network_value int: value to match against to indicate in-network
        suffix str: optional param to add suffix to network_flag (if creating >1 per table, eg)
  
    """
    
    return f"""case when {network_col} = {network_value} then 'In-Network'
                    when {network_col} is not null then 'Out-of-Network'
                    else null
             end as network_flag{suffix}
         """

def assign_fac_types(alias, current_col='FirmTypeName', new_col='facility_type'):
    """
    Function assign_fac_types to return sql text to create facility_type col from input firmtype
    params:
        alias str: alias for table to get FirmTypeName from
        current_col str: optional param for name of firm type existing column, default = FirmTypeName
        new_col str: optional param for name of new column, default = facility_type
    
    returns:
        sql case statement
    
    """
    
    return f"""
        case when {alias}.{current_col} in ('Ambulatory Surgery Center', 'Hospital', 'Imaging Center', 'Physician Group',
                                            'Renal Dialysis Facility', 'Retail Clinic', 'Urgent Care Clinic')
                                         
               then {alias}.{current_col}
               
               when {alias}.{current_col} in ('Assisted Living Facility', 'Home Health Agency', 'Hospice', 'Skilled Nursing Facility')
               then 'Post-Acute'
               
               else null
               end as {new_col}
       """