# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider OA Measures: Bulk submit batch provider run
# MAGIC 
# MAGIC **Program:** _batch_provider_oa_measures
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Bulk submit to call driver program for Provider OA measures once per facility/radius/date entry <br>

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

# create func decorated with timeit to print elapsed time for each call to batch notebook

@timeit
def run_batch(arguments_dict):
    dbutils.notebook.run('_batch_provider_oa_measures', 0, 
                     arguments=arguments_dict)

# COMMAND ----------

# create all widgets (including optional input table)

RUN_VALUES = get_widgets(include_widgets = list(range(0,9)))

# COMMAND ----------

INPUT_TABLE = dbutils.widgets.get("(8) Input Table")

INPUTS = spark.sql(f"""select distinct DEFHC_ID, RADIUS, START_DATE, END_DATE, LT18, INT(SORT) as SORT from {INPUT_TABLE} ORDER BY INT(SORT)""").collect()

DEFHC_ID_LIST = [r['DEFHC_ID'] for r in INPUTS]
RADIUS_LIST = [r['RADIUS'] for r in INPUTS]
START_DATE_LIST = [r['START_DATE'] for r in INPUTS]
END_DATE_LIST = [r['END_DATE'] for r in INPUTS]
LT18_LIST = [r['LT18'] for r in INPUTS]
SORT_LIST = [r['SORT'] for r in INPUTS]

# COMMAND ----------

for i, DEFHC_ID in enumerate(DEFHC_ID_LIST):
    RADIUS = RADIUS_LIST[i]
    START_DATE = START_DATE_LIST[i]
    END_DATE = END_DATE_LIST[i]
    LT18 = LT18_LIST[i]
    SORT = SORT_LIST[i]
    
    process_stmt = lambda n, x: f"{n}: {x} process for ID {DEFHC_ID}, with a {RADIUS} mile radius, for claims between {START_DATE} and {END_DATE}. Less than 18 filter = {LT18}"
    
    if SORT < 15:
    
        print(process_stmt(n=SORT, x='SKIPPING'))
        
    else:
        
        print(process_stmt(n=SORT, x='RUNNING'))
        
        run_batch(arguments_dict={RUN_VALUES['RUN_SETUP'][0]: 0, 
                                 RUN_VALUES['DEFHC_ID'][0]: DEFHC_ID,
                                 RUN_VALUES['RADIUS'][0]: RADIUS,
                                 RUN_VALUES['START_DATE'][0]: START_DATE,
                                 RUN_VALUES['END_DATE'][0]: END_DATE,
                                 RUN_VALUES['DATABASE'][0]: 'ds_provider',
                                 RUN_VALUES['RUN_QC'][0]: 0,
                                 RUN_VALUES['SUBSET_LT18'][0]: LT18})
