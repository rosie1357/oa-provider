# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include funcs to calculate bar/pie chart numbers

# COMMAND ----------

def get_top_values(intable, defhc, defhc_value, max_row, strat_cols, subset=''):
    """
    Function get_top_values() to get aggregate claim counts for either pie chart (by network) or bar chart (by facility)
        used for either hospital/ASC charts (dashboard) or market share (facilities page)
        will get count of claims by strat_col and defhc_id or net_defhc_id
        will create collapsed/formatted names and labels to collapse >4th or >5th positions (pie or bar, respectively)
        
    params:
        intable str: table to read from
        defhc str: prefix to id and name cols to specify network or facility, = 'net_defhc' for network, 'defhc' for facility
        defhc_value int: value for either your network or your facility
        max_row int: value for max row to keep, all others are collapsed (4 or 5)
        strat_cols list: list of col(s) to stratify by
        subset str: optional param to give additional subset for input table, default='' (no subset)
        
    returns:
        spark df with claim counts at network or facility level
    
    """
    
    # assign place_name based on defhc (whether specified as defhc or net_defhc)
    
    if defhc == 'defhc':
        place_name = 'Facility'
        
    elif defhc == 'net_defhc':
        place_name = 'Network'
        
    strat_cols = ', '.join(strat_cols)
    
    return spark.sql(f"""

        select *
               
               -- create collapsed/formatted values based on max row number to collapse, and label formatted specifically for your network/facility
            
               ,case when rn < {max_row} then {defhc}_id 
                     else Null
                     end as {defhc}_id_collapsed

               ,case when rn < {max_row} then {defhc}_name
                     else 'Other' 
                     end as {place_name.lower()}_name

              ,case when {defhc}_id = {defhc_value} then "1. Your {place_name}"
                    when rn < {max_row} then concat(rn + 1, ". {place_name} ", rn + 1)
                    else "{max_row+1}. Other"
                    end as {place_name.lower()}_label

        from (

            select *
            
                   -- create rn to order networks separately for hospital/ASC, with your network as 0, null IDs as 100 (ignore), and all others by claim counts
                   
                   ,case when {defhc}_id = {defhc_value} then 0 
                         when {defhc}_id is null then 100
                         else row_number() over (partition by {strat_cols}
                                                 order by case when {defhc}_id is null or {defhc}_id = {defhc_value} then 0 
                                                               else cnt_claims 
                                                               end desc) 
                          end as rn

           from (
                select {defhc}_id
                       , {defhc}_name as
                       , {strat_cols}
                       , count(*) as cnt_claims

                from {intable}
                {subset}
                group by {defhc}_id
                       , {defhc}_name
                       , {strat_cols}
                ) a
            ) b
        """)
