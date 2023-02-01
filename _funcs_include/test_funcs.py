# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include all test funcs to exit with error for provider dashboard

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/sdf_print_comp_funcs

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/notebook_pipeline_funcs

# COMMAND ----------

def test_distinct(sdf, name, cols):
    """
    Function test_distinct to call sdf_check_distinct() on given sdf and cols,
        and exit notebook with error if NOT distinct
    
    params:
        sdf: spark dataframe
        cols list: list of cols to check distinct by
        
   returns:
       if distinct, only prints message
       if not distinct, dbutils.notebook.exit() with dictionary of return params
        
    """
    
    distinct_return = sdf_check_distinct(sdf, cols)
    print(distinct_return)
    
    if 'NOT Distinct' in distinct_return:
        
        exit_notebook(f"ERROR: Duplicate records by {', '.join(cols)} in table '{name}'")
