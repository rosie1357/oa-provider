# Databricks notebook source
# MAGIC %md 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC
# MAGIC ## Provider OA Measures: Bulk submit batch provider run
# MAGIC  
# MAGIC **Program:** _batch_provider_oa_measures
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Bulk submit to call driver program for Provider OA measures once per facility/radius/date entry

# COMMAND ----------

# MAGIC %run ./_funcs/_paths_include

# COMMAND ----------

from _general_funcs.decorators import timeit

from _funcs.setup_funcs import get_widgets, return_widget_values, return_run_status
from _funcs.params import STATUS_TBL

# COMMAND ----------

# create func decorated with timeit to print elapsed time for each call to batch notebook

@timeit()
def run_batch(arguments_dict):
    
    returns = dbutils.notebook.run('_batch_provider_oa_measures', 0, arguments=arguments_dict)

    print(returns)

# COMMAND ----------

# create all widgets (including optional input table, rerun indicator)
# set hard-coded database value

RUN_VALUES = get_widgets(include_widgets = list(range(0,10)))
DATABASE = 'ds_provider'

# COMMAND ----------

# get values for input table and rerun indicator, create pandas df from input table

INPUT_TABLE, RERUN_EXISTING = return_widget_values(RUN_VALUES, ['INPUT_TABLE', 'RERUN_EXISTING'])

INPUTS_DF = spark.sql(f"""select distinct DEFHC_ID, RADIUS, START_DATE, END_DATE, LT18, SORT from {INPUT_TABLE} ORDER BY SORT""").toPandas()

# COMMAND ----------

# loop over every row of input table to process
# if RERUN_EXISTING = 0 (do NOT rerun if any successful runs of the same values have been run before), must first check if runs exist and skips

for index, row in INPUTS_DF.iterrows():

    DEFHC_ID, RADIUS, START_DATE, END_DATE, LT18, SORT = row[['DEFHC_ID', 'RADIUS', 'START_DATE', 'END_DATE', 'LT18', 'SORT']]
    process_stmt = lambda n, x: '\n' + f"{n}: {x} run for ID {DEFHC_ID}, radius = {RADIUS}, {START_DATE} - {END_DATE}, <18 filter = {LT18}"

    if RERUN_EXISTING == 0:

        # must check if SUCCESSFUL run of the same combos exists, if so, skip run

        status, date = return_run_status(DATABASE, STATUS_TBL, DEFHC_ID, RADIUS, START_DATE, END_DATE, LT18)
        if status == True:
            print(process_stmt(n=SORT, x='SKIPPING'))
            print('\t' + f"(Run with no errors on {date.date()})")
            continue

        # if found but UNSUCCESSFUL, will rerun

        elif status == False:
            print(process_stmt(n=SORT, x='FOUND PRIOR'))
            print('\t' + f"(Run with errors on {date.date()}, will rerun)")

    print(process_stmt(n=SORT, x='PROCESSING'))
    
    run_batch(arguments_dict={RUN_VALUES['RUN_SETUP'][0]: 0, 
                               RUN_VALUES['DEFHC_ID'][0]: DEFHC_ID,
                               RUN_VALUES['RADIUS'][0]: RADIUS,
                               RUN_VALUES['START_DATE'][0]: START_DATE,
                               RUN_VALUES['END_DATE'][0]: END_DATE,
                               RUN_VALUES['DATABASE'][0]: DATABASE,
                               RUN_VALUES['RUN_QC'][0]: 0,
                               RUN_VALUES['SUBSET_LT18'][0]: LT18})
