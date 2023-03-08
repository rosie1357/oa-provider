# Databricks notebook source
# scratch notebook to investigate tables with 0 counts for some runs

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/oa_provider/_funcs_include/all_provider_funcs

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from ds_provider.record_counts where count=0 order by defhc_id

# COMMAND ----------


