# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include all base/setup functions for provider dashboard

# COMMAND ----------

from operator import itemgetter

# COMMAND ----------

def get_widgets(include_widgets = list(range(1,7))):
    """
    Function get_widgets() to 
        a) create included widgets if not already exist on notebook (first time running)
        b) get and return a dictionary of values for all included widgets
        
    params:
        include_widgets list: optional list of integers to specify widgets by number to request which to include, default = 1-6
        
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
