from operator import itemgetter
from pyspark.sql import SparkSession
from _general_funcs.utils import get_dbutils

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
                       'clean_func': lambda x: int(x)},
    
                    8: {'name': "(8) Input Table",
                       'default': "",
                       'var': 'INPUT_TABLE',
                       'clean_func': lambda x: x},
                   
                   9: {'name': "(9) Rerun Existing",
                       'default': "1",
                       'var': 'RERUN_EXISTING',
                       'clean_func': lambda x: int(x)}
                  }
    
    for w_num in include_widgets:
        
        name, default, var, clean_func = itemgetter('name', 'default', 'var', 'clean_func')(all_widgets[w_num])
        
        get_dbutils().widgets.text(name, default)
        
        values_dict[var] = (name, clean_func(get_dbutils().widgets.get(name)))
    
    return values_dict


def return_widget_values(widget_dict, vars):
    
    returns = []
    for var in vars:
        returns += [widget_dict[var][1]]
        
    return tuple(returns)


def return_run_status(db, tbl, defhc_id, radius, start_date, end_date, lt18):
    """
    Function return_run_status to take in given set of params and return tuple with success and current_dt values
    params:
        db str: name of database
        tbl str: name of table with run status
        defhc_id int: facility ID
        radius int: radius (miles)
        start_date str: start date in form of YYYY-MM-DD
        end_date str: end date in form of YYYY-MM-DD
        lt18 int: indicator to subset to lt18 (0/1)
        
    returns:
        tuple with success, current_dt values IF record found
        if zero records found, print message and return tuple of None, None
    
    """

    spark = SparkSession.getActiveSession()
    
    status = spark.sql(f"""select success, current_dt from {db}.{tbl} 
                           where defhc_id={defhc_id} and radius={radius} and start_date='{start_date}' and end_date='{end_date}' and subset_lt18={lt18}
                           order by current_dt desc
                           limit 1
                       """).collect()
    if len(status) == 0:
        return None, None
    
    return status[0]['success'], status[0]['current_dt']