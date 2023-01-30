# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider OA Measures: Batch Submit
# MAGIC 
# MAGIC **Program:** _batch_provider_oa_measures
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Driver program to submit all notebooks for creation of provider OA metrics <br>

# COMMAND ----------

import ast

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs

# COMMAND ----------

RUN_VALUES = get_widgets(include_widgets = list(range(0,7)))
RUN_ARGUMENTS = {v[0]: v[1] for k, v in RUN_VALUES.items()}

RUN_SETUP, DATABASE = return_widget_values(RUN_VALUES, ['RUN_SETUP' , 'DATABASE'])

# COMMAND ----------

clear_database(GET_TMP_DATABASE(DATABASE))

# COMMAND ----------

# run initial setup notebook if requested

if RUN_SETUP == 1:

    returns = ast.literal_eval(dbutils.notebook.run('000_initial_setup', 0, arguments = RUN_ARGUMENTS))

    notebook_returns_passthrough(returns_dict = returns,
                                 pass_message = returns['message']
                                )

# COMMAND ----------

# run notebook for main table creation

returns = ast.literal_eval(dbutils.notebook.run('001_create_tables', 0, arguments = RUN_ARGUMENTS))

notebook_returns_passthrough(returns_dict = returns,
                             pass_message = returns['message']
                            )

# COMMAND ----------

# run notebook for page 1 (dashboard) measure creation
# TODO: add specific returns

dbutils.notebook.run('002_dashboard', 0, arguments = RUN_ARGUMENTS)

# COMMAND ----------

# run notebook for page 3 (specialists) measure creation
# TODO: add specific returns

dbutils.notebook.run('004_specialists', 0, arguments = RUN_ARGUMENTS)

# COMMAND ----------

# run notebook for page 4 (PCPs) measure creation
# TODO: add specific returns

dbutils.notebook.run('006_pcps', 0, arguments = RUN_ARGUMENTS)
