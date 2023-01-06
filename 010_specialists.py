# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![logo](/files/ds_dhc_logo_small.png)
# MAGIC 
# MAGIC ## Provider Dashboard: 010 Specialists
# MAGIC 
# MAGIC **Program:** 010_specialists
# MAGIC <br>**Authors:** Katie May, Rosie Malsberger
# MAGIC <br>**Date:** January 2023
# MAGIC <br>
# MAGIC <br>
# MAGIC **Description:** Program to create and save metrics for provider specialist page <br>
# MAGIC <br>
# MAGIC **NOTE**: DATABASE and TMP_DATABASE params below are value extracted from database widget, value passed to TMP_DATABASE() lambda func param, tbl var names specified in params
# MAGIC 
# MAGIC **Inputs**:
# MAGIC   - {TMP_DATABASE}.input_org_info
# MAGIC   - {TMP_DATABASE}.nearby_hcos
# MAGIC   - {TMP_DATABASE}.nearby_hcps
# MAGIC   - {TMP_DATABASE}.nearby_npis
# MAGIC   - {TMP_DATABASE}.{MX_CLMS_TBL}
# MAGIC   - {TMP_DATABASE}.{PCP_REFS_TBL}
# MAGIC   - {TMP_DATABASE}.{AFF_CLMS_TBL}
# MAGIC   - MartDim.D_Organization
# MAGIC   - MxMart.F_MxClaim_v2

# COMMAND ----------

# MAGIC %run ./_funcs_include/all_provider_funcs
