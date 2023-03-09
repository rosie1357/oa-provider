# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include all test funcs to exit with error for provider dashboard

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/sdf_print_comp_funcs

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/sdf_funcs

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/notebook_pipeline_funcs

# COMMAND ----------

def test_distinct(defhc_id, radius, start_date, end_date, subset_lt18, sdf, name, cols, to_subset=True):
    """
    Function test_distinct to call sdf_check_distinct() on given sdf and cols,
        and exit notebook with error if NOT distinct
    
    params:
        defhc_id int: input facility id
        radius int: input radius
        start_date str: input start_date
        end_date str: input end_date
        subset_lt18 int: indicator for subset to lt18, = 0 or 1
        sdf: spark dataframe
        name str: name of table for print statement
        cols list: list of cols to check distinct by
        to_subset bool: optional param to specify if need to subset table to given id/radius/dates, default = False
        
   returns:
       if distinct, only prints message
       if not distinct, dbutils.notebook.exit() with dictionary of return params
        
    """
    
    if to_subset:
        
        sdf = sdf.filter(F.col('input_defhc_id')==defhc_id) \
                 .filter(F.col('radius')==radius) \
                 .filter(F.col('start_date')==start_date) \
                 .filter(F.col('end_date')==end_date) \
                 .filter(F.col('subset_lt18')==subset_lt18)
    
    distinct_return = sdf_check_distinct(sdf, cols)
    print(distinct_return)
    
    if 'NOT Distinct' in distinct_return:
        
        exit_notebook(f"ERROR: Duplicate records by {', '.join(cols)} in table '{name}'")

# COMMAND ----------

def test_widgets_match(widget_values, match_tbl, match_cols):
    """
    Function test_widgets_match to test the given widget values match the current values in the input org table,
        and exit notebook with error if not
        
    params:
        widget_values list: list of values to test
        match_tbl str: name of input database table to pull
        match_cols list: list of cols to pull value from to match
        
    returns:
        if match, only prints that values match
        if not match, dbutils.notebook.exit() with message that do not match
    
    """
    
    match_values = sdf_return_row_values(hive_to_df(match_tbl), match_cols)
    
    match = all([x == y for x, y in zip(widget_values,match_values)])
    
    if match:
        print(f"Widget and org table values for ({', '.join(match_cols)}) match")
        
    else:
        exit_notebook(f"Widget ({', '.join(map(str,widget_values))}) and org table ({', '.join(map(str,match_values))}) values for ({', '.join(match_cols)}), respectively, do not match")
