# Databricks notebook source
"""
scratch notebook to backfill info for prior runs before implementing additional features
    1. add subset_lt18 columns and fill with given facility's value
    2. populate counts table
    3. populate run status table

"""

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Set table name/run values params

# COMMAND ----------

# get list of ALL 'page' tables

all_tables = spark.sql("show tables in ds_provider").collect()
page_tables = [t[1] for t in all_tables if t[1].startswith('page')]

page_tables

# COMMAND ----------

# create list of ID-specific tables in each facility's database

db_tables = ['input_org_info','nearby_hcos_id','nearby_hcps','nearby_hcos_npi',
             f"{MX_CLMS_TBL}",f"{PCP_REFS_TBL}",'inpat90_dashboard','inpat90_facilities']

# COMMAND ----------

# read in input table to get all unique values for ID and LT18 (will confirm each ID only has one value for LT18)

INPUT_TABLE = 'ds_provider.provider_oa_inputs_v1'

INPUTS = spark.sql(f"""select distinct DEFHC_ID, RADIUS, START_DATE, END_DATE, LT18, INT(SORT) as SORT from {INPUT_TABLE} ORDER BY INT(SORT)""")

INPUTS.select('DEFHC_ID', 'LT18').distinct().count(), INPUTS.select('DEFHC_ID').distinct().count()

# COMMAND ----------

# put into pandas df, add MGH (one additional facility not in input list)

RUNS_DF = pd.concat([INPUTS.toPandas(), 
                     pd.DataFrame(data={'DEFHC_ID': '1973',
                                        'RADIUS': '5',
                                        'START_DATE': '2021-11-01',
                                        'END_DATE': '2022-10-31',
                                        'LT18': '0',
                                        'SORT': 50}, index=[0])]).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Create/fill LT18 columns

# COMMAND ----------

# create statement to add empty col on individual database table at needed position, and to fill with given value

def create_subset_col(id, table, subset_lt18, page_table=False):
    
    db = "ds_provider"
    match_col = 'defhc_id'

    if page_table == False:
        
        db = f"ds_provider_{id}"
        match_col = "input_defhc_id"
        
        # skip if column already exists (if rerunning to avoid inserting more than once)
        
        if not 'subset_lt18' in hive_tbl_cols(f"{db}.{table}", as_list=True):
        
            spark.sql(f"alter table {db}.{table} add column subset_lt18 integer after end_date")
    
    spark.sql(f"update {db}.{table} set subset_lt18 = {subset_lt18} where {match_col} = '{id}'")

# COMMAND ----------

for index, row in RUNS_DF.iterrows():

    defhc_id, subset_lt18 = row['DEFHC_ID'], row['LT18']
    print(f"{index+1}. ID = {defhc_id}, subset = {subset_lt18}")
    
    for page_table in page_tables:

         create_subset_col(id = defhc_id, 
                           table = page_table, 
                           subset_lt18 = int(subset_lt18),
                           page_table=True)

    for db_table in db_tables:

        create_subset_col(id = defhc_id, 
                           table = db_table, 
                           subset_lt18 = int(subset_lt18))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Add record counts to counts table

# COMMAND ----------

# copy insert_into_output function and remove most of it to ONLY get table counts and insert into table_counts table

def insert_into_output_partial(defhc_id, radius, start_date, end_date, subset_lt18, counts_table, sdf, table, must_exist=True, maxrecs=25, id_prefix=''):
    """
    Function insert_into_output() to insert new data into table, 
        first checking if there are records existing for given id/radius/dates, and if so, deleting before insertion
        
    params:
        defhc_id int: input facility id
        radius int: input radius
        start_date str: input start_date
        end_date str: input end_date
        subset_lt18 int: indicator for subset lt18
        counts_table str: name of table to insert count of records into
        sdf spark df: spark df with records to be inserted (will insert all rows, all columns)
        table str: name of output table 
        must_exist bool: optional param to specify table must exist before run, default = True
            if not, will create if does NOT exist and will print message that created
        maxrecs int: optional param to specify max recs to display, default = 25
        id_prefix str: optional param to specify prefix on defhc_id (used for initial table creation, will be input_defhc_id), default=''
    
    returns:
        count of inserted records
        has optional deletion before insertion, will print deleted/inserted dates
    
    """            
    
    # identify if same set of outputs exists in table - if so, print date and delete
    
    condition = f"""
        {id_prefix}defhc_id = {defhc_id} and 
        radius = {radius} and 
        start_date = '{start_date}' and 
        end_date = '{end_date}' and
        subset_lt18 = {subset_lt18}
        """
    
    # only run commands to select/delete from given table IF table must exist OR must not exist but already exists
    
    database, table_name = table.split('.')[0], table.split('.')[1]
    
    tbl_count = hive_tbl_count(table, condition = f"where {condition}")
    
    # create sdf with base cols, count and time stamp, then call populate_most_recent() to set any remaining recs to False and insert new rec
    
    counts_sdf = create_final_output(base_sdf = base_output_table(defhc_id, radius, start_date, end_date, subset_lt18), 
                                     counts_sdf = spark.createDataFrame(pd.DataFrame(data={'database': database,
                                                                                           'table_name': table_name,
                                                                                           'count': tbl_count}, index=[0]))
                                    )
    
    # to pass condition, must REMOVE id_prefix if not empty (no prefix on most recent table)
    
    condition = condition.replace(f"{id_prefix}defhc_id", 'defhc_id')
    
    populate_most_recent(sdf = counts_sdf,
                         table = counts_table,
                         condition = f"{condition} and database = '{database}' and table_name = '{table_name}'")

# COMMAND ----------

for index, row in RUNS_DF.iterrows():

    defhc_id, radius, start_date, end_date, subset_lt18 = row['DEFHC_ID'], row['RADIUS'], row['START_DATE'], row['END_DATE'], row['LT18']
    print(f"{index+1}. ID = {defhc_id}, radius = {radius}, dates = {start_date} - {end_date}, subset = {subset_lt18}")

    for table in db_tables:

        insert_into_output_partial(defhc_id = defhc_id, radius = radius, start_date = start_date, end_date = end_date, subset_lt18 = subset_lt18,
                                   counts_table = 'ds_provider.record_counts', sdf = INPUTS, table = f"ds_provider_{defhc_id}.{table}", id_prefix='input_')
        
    for table in page_tables:

        insert_into_output_partial(defhc_id = defhc_id, radius = radius, start_date = start_date, end_date = end_date, subset_lt18 = subset_lt18,
                                   counts_table = 'ds_provider.record_counts', sdf = INPUTS, table = f"ds_provider.{table}")


# COMMAND ----------

insert_cols = hive_tbl_cols('ds_provider.run_status')

for index, row in RUNS_DF.iterrows():
    
    defhc_id, radius, start_date, end_date, subset_lt18 = row['DEFHC_ID'], row['RADIUS'], row['START_DATE'], row['END_DATE'], row['LT18']
    print(f"{index+1}. ID = {defhc_id}, radius = {radius}, dates = {start_date} - {end_date}, subset = {subset_lt18}")
    
    if defhc_id != '5714':
        
        spark.sql(f"""insert into ds_provider.run_status ({insert_cols})
                        select {insert_cols}
                        from (select defhc_id, radius, start_date, end_date, subset_lt18, run_number, current_dt, True as most_recent, True as success, Null as fail_reason
                                            from ds_provider.record_counts
                                            where defhc_id = {defhc_id} and 
                                                 radius = {radius} and 
                                                 start_date = '{start_date}' and 
                                                 end_date = '{end_date}' and
                                                 subset_lt18 = {subset_lt18} and
                                                 database = 'ds_provider'
                                            limit 1
                              ) a
                    """)
