# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **credentials.py: This notebook gets aws credentials from secrets**

# COMMAND ----------

AWS_CREDS = {
    'aws_access_key_id': dbutils.secrets.get(scope = 'ds_credentials', key = 'aws-oa-access-key'),
    'aws_secret_access_key': dbutils.secrets.get(scope = 'ds_credentials', key = 'aws-oa-secret-access-key')
}
