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

RUN_SETUP, DATABASE, DEFHC_ID = return_widget_values(RUN_VALUES, ['RUN_SETUP' , 'DATABASE', 'DEFHC_ID'])

# message to print on return of each notebook with output counts on pass

COUNTS_MESSAGE = lambda d, k, t: f"All tables with {t} counts:" + '\n\t' + '\n\t'.join({f"{k}: {v:,d}" for k,v in d.get(k,{}).items()})

# COMMAND ----------

# create database for given ID if not already exists

FAC_DATABASE = GET_FAC_DATABASE(DATABASE, DEFHC_ID)

spark.sql(f"create database if not exists {FAC_DATABASE}").display()

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
                             pass_message = COUNTS_MESSAGE(returns, 'all_counts', 'total')
                            )

# COMMAND ----------

# run notebook for page 1 (dashboard) measure creation

returns = ast.literal_eval(dbutils.notebook.run('002_dashboard', 0, arguments = RUN_ARGUMENTS))

notebook_returns_passthrough(returns_dict = returns,
                             pass_message = COUNTS_MESSAGE(returns, 'all_counts', 'inserted')
                            )

# COMMAND ----------

# run notebook for page 3 (specialists) measure creation

returns = ast.literal_eval(dbutils.notebook.run('004_specialists', 0, arguments = RUN_ARGUMENTS))

notebook_returns_passthrough(returns_dict = returns,
                             pass_message = COUNTS_MESSAGE(returns, 'all_counts', 'inserted')
                            )

# COMMAND ----------

# run notebook for page 4 (PCPs) measure creation

returns = ast.literal_eval(dbutils.notebook.run('005_pcps', 0, arguments = RUN_ARGUMENTS))

notebook_returns_passthrough(returns_dict = returns,
                             pass_message = COUNTS_MESSAGE(returns, 'all_counts', 'inserted')
                            )

# COMMAND ----------

# run notebook for page 5 (facilities) measure creation

returns = ast.literal_eval(dbutils.notebook.run('006_facilities', 0, arguments = RUN_ARGUMENTS))

notebook_returns_passthrough(returns_dict = returns,
                             pass_message = COUNTS_MESSAGE(returns, 'all_counts', 'inserted')
                            )
