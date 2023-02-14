# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include all base/setup functions for provider dashboard

# COMMAND ----------

from operator import itemgetter

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/fs_funcs

# COMMAND ----------

# MAGIC %run ./params

# COMMAND ----------

def get_widgets(include_widgets = list(range(1,8))):
    """
    Function get_widgets() to 
        a) create included widgets if not already exist on notebook (first time running)
        b) get and return a dictionary of values for all included widgets
        
    params:
        include_widgets list: optional list of integers to specify widgets by number to request which to include, default = 1-7
        
    returns:
        dictionary with included entries, with key = var name, value = tuple (widget name, CURRENT widget value (will only use defaults first time through))
    """
    
    values_dict = {}
    
    all_widgets = {0: {'name': "(0) Run Initial Setup",
                       'default': "0",
                       'var': 'RUN_SETUP',
                       'clean_func': lambda x: int(x)},
                   
                   1: {'name': "(1) Definitive ID",
                       'default': "0000",
                       'var': 'DEFHC_ID',
                       'clean_func': lambda x: int(x)},
                   
                   2: {'name': "(2) Radius (miles)",
                       'default': "0",
                       'var': 'RADIUS',
                       'clean_func': lambda x: int(x)},
                   
                   3: {'name': "(3) Start Date",
                       'default': "2021-01-01",
                       'var': 'START_DATE',
                       'clean_func': lambda x: x},
                   
                   4: {'name': "(4) End Date",
                      'default': "2021-12-31",
                      'var': 'END_DATE',
                      'clean_func': lambda x: x},
                   
                   5: {'name': "(5) Database",
                       'default': "",
                       'var': 'DATABASE',
                       'clean_func': lambda x: x},
                   
                   6: {'name': "(6) Run QC Checks",
                       'default': "0",
                       'var': 'RUN_QC',
                       'clean_func': lambda x: int(x)},
                   
                   7: {'name': "(7) Under 18 Subset",
                       'default': "0",
                       'var': 'SUBSET_LT18',
                       'clean_func': lambda x: int(x)}
                  }
    
    for w_num in include_widgets:
        
        name, default, var, clean_func = itemgetter('name', 'default', 'var', 'clean_func')(all_widgets[w_num])
        
        dbutils.widgets.text(name, default)
        
        values_dict[var] = (name, clean_func(dbutils.widgets.get(name)))
    
    return values_dict

# COMMAND ----------

def return_widget_values(widget_dict, vars):
    
    returns = []
    for var in vars:
        returns += [widget_dict[var][1]]
        
    return tuple(returns)

# COMMAND ----------

def create_views(defhc_id, radius, start_date, end_date, database, tables, id_prefix=''):
    """
    Function create_views to create temp views for all given tables with subset
        to given id/radius/dates
        
     params:
        defhc_id int: input facility id
        radius int: input radius
        start_date str: input start_date
        end_date str: input end_date
        database str: name of database with all tables
        tables list: list of all tables to make to views
        id_prefix str: optional param to specify prefix on defhc_id (used for initial table creation, will be input_defhc_id), default=''   
        
    returns:
        none, print of views created
        
    will error if count of any table == 0
    
    """
    
    for tbl in tables:
        
        spark.sql(f"""
            create or replace temp view {tbl}_vw as
            select * 
            from {database}.{tbl}
            where {id_prefix}defhc_id = {defhc_id} and 
                  radius = {radius} and 
                  start_date = '{start_date}' and 
                  end_date = '{end_date}'
        """)
        cnt = hive_tbl_count(f"{tbl}_vw")
        
        assert cnt >0, f"ERROR: table {database}.{table} has 0 records for given id/radius/time frame"
        
        print(f"{tbl}_vw created with {cnt:,d} records")
