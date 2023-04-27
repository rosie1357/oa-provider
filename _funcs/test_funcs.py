from _general_funcs.notebook_pipeline_funcs import exit_notebook
from _general_funcs.sdf_funcs import sdf_return_row_values

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