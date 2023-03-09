# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook with main provider class

# COMMAND ----------

# MAGIC %run ./params

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/fs_funcs

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/sdf_funcs

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

class ProviderRun(object):
    
    def __init__(self, defhc_id, radius, start_date, end_date, subset_lt18, database, run_qc, 
                 base_output_prefix = '', 
                 db_tables = ALL_TABLES,
                 counts_table = COUNTS_TBL,
                 status_table = STATUS_TBL):
        
        self.defhc_id = defhc_id
        self.radius = radius
        self.start_date = start_date
        self.end_date = end_date
        self.subset_lt18 = subset_lt18
        self.database = database
        self.run_qc = run_qc
        self.base_output_prefix = base_output_prefix
        self.db_tables = db_tables
        self.counts_table = counts_table
        self.status_table = status_table
        
        # create name for facility-specific database
        self.fac_database = f"{self.database}_{self.defhc_id}"
        
        # create base output table to join all other outputs to
        self.base_output_table = self.base_output_table()
        
        # create empty dict to save table counts
        self.table_counts = {}
        
    def condition_stmt(self, id_prefix):
        
        return f""" {id_prefix}defhc_id = {defhc_id} and 
                    radius = {radius} and 
                    start_date = '{start_date}' and 
                    end_date = '{end_date}' and
                    subset_lt18 = {subset_lt18}
                    """
        
    def base_output_table(self):
        """
        method base_output_table to return a one-rec spark df to join to all output tables to keep same columns on every table
        
        returns:
            one-rec spark df with above four columns

        """

        df = pd.DataFrame(data = {f"{self.base_output_prefix}defhc_id": self.defhc_id,
                                 'radius': self.radius,
                                 'start_date': self.start_date,
                                 'end_date': self.end_date,
                                 'subset_lt18': self.subset_lt18
                                 },
                                 index=[0]
                         )

        return spark.createDataFrame(df)
    
    def create_final_output(self, counts_sdf):
        """
        method create_final_output to read in base and counts spark dfs to add base columns to counts, and add final time stamp
        params:
            counts_sdf sdf: sdf with all counts

        returns:
            spark df with base_sdf cols, counts_sdf cols, and current_dt
        """

        return self.base_output_table.join(counts_sdf).withColumn('current_dt', F.current_timestamp())
    
    def populate_most_recent(sdf, table, condition):
        """
        method populate_most_recent to identify any recs in given table to set as most_recent=False and get run count before inserting recent recs
        params:
            sdf spark df: sdf to insert (without most_recent col)
            table str: name of table to update
            condition str: where stmt (without leading 'where') to identify old recs to set as most_recent=False

        returns:
            none
        """

        # first, identify if there are any existing counts for the same condition, and set most_recent = False

        spark.sql(f"update {table} set most_recent = False where {condition}")

        # second, count the number of existing records in the same for same condition to create run_number

        prior_runs_count = hive_tbl_count(table, condition = f"where {condition}")

        # get cols to populate in perm table

        insert_cols = hive_tbl_cols(table)

        # create view and insert into table

        sdf.createOrReplaceTempView('sdf_vw')

        spark.sql(f"""
        insert into {table} ({insert_cols})
        select {insert_cols}
        from (
            select *
                   , True as most_recent
                   , {prior_runs_count+1} as run_number 
            from sdf_vw
            ) a
        """)

    def insert_into_output(sdf, table, must_exist=True, maxrecs=25, id_prefix=''):
        """
        method insert_into_output() to insert new data into table, 
            first checking if there are records existing for given id/radius/dates, and if so, deleting before insertion

        params:
            sdf spark df: spark df with records to be inserted (will insert all rows, all columns)
            table str: name of output table 
            must_exist bool: optional param to specify table must exist before run, default = True
                if not, will create if does NOT exist and will print message that created
            maxrecs int: optional param to specify max recs to display, default = 25
            id_prefix str: optional param to specify prefix on defhc_id (used for initial table creation, will be input_defhc_id), default=''

        returns:
            none (prints sample recs/counts deleted and/or added)

        """
        
        # create condition stmt
        
        condition = self.condition_stmt(id_prefix)

        # create view from sdf

        sdf.createOrReplaceTempView('sdf_vw')

        # only run commands to select/delete from given table IF table must exist OR must not exist but already exists

        database, table_name = table.split('.')[0], table.split('.')[1]

        table_exists = spark._jsparkSession.catalog().tableExists(database, table_name)

        if must_exist or (must_exist==False and table_exists):

            old_dts = spark.sql(f"""
                select distinct current_dt
                from {table}
                where {condition}
                """).collect()

            if len(old_dts) > 0:
                print(f"Old records found, will delete: {', '.join([str(dt[0]) for dt in old_dts])}")
            else:
                print("No old records found to delete")

            spark.sql(f"""
                delete
                from {table}
                where {condition}
                """)

            # now insert new data, print all new records

            insert_cols = ', '.join(sdf.columns)

            spark.sql(f"""
                insert into {table} ({insert_cols})
                select * from sdf_vw
                """)
        else:

            print(f"Table {table} did NOT exist - creating now")

            spark.sql(f"""
                create table {table} as 
                select * from sdf_vw        
                """)

        print(f"New records inserted (max {maxrecs}):")

        spark.sql(f"""
           select *
           from {table}
           where {condition}
           limit {maxrecs}
           """).display()

        tbl_count = hive_tbl_count(table, condition = f"where {condition}")
        
        # update table counts
        self.table_counts[table] = tbl_count

        # create sdf with base cols, count and time stamp, then call populate_counts() to set any remaining recs to False and insert new rec

        counts_sdf = self.create_final_output(counts_sdf = spark.createDataFrame(pd.DataFrame(data={'database': database,
                                                                                                    'table_name': table_name,
                                                                                                    'count': tbl_count}, 
                                                                                              index=[0]))
                                             )

        # to pass condition, must REMOVE id_prefix if not empty (no prefix on most recent table)

        condition = condition.replace(f"{id_prefix}defhc_id", 'defhc_id')

        populate_most_recent(sdf = counts_sdf,
                             table = counts_table,
                             condition = f"{condition} and database = '{database}' and table_name = '{table_name}'")


    def create_views(self, id_prefix='input_'):
        """
        method create_views to create temp views for all given tables with subset
            to given id/radius/dates

         params:
            id_prefix str: optional param to specify prefix on defhc_id (used for initial table creation, will be input_defhc_id), default='input_'   

        returns:
            none, print of views created

        will error if count of any table == 0

        """

        for tbl in self.db_tables:

            spark.sql(f"""
                create or replace temp view {tbl}_vw as
                select * 
                from {self.database}.{tbl}
                where {id_prefix}defhc_id = {self.defhc_id} and 
                      radius = {self.radius} and 
                      start_date = '{self.start_date}' and 
                      end_date = '{self.end_date}' and
                      subset_lt18 = {self.subset_lt18}
            """)
            cnt = hive_tbl_count(f"{tbl}_vw")

            assert cnt >0, f"ERROR: table {self.database}.{table} has 0 records for given id/radius/time frame"

            print(f"{tbl}_vw created with {cnt:,d} records")
            
    def test_distinct(sdf, name, cols, to_subset=True):
        """
        method test_distinct to call sdf_check_distinct() on given sdf and cols,
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

            sdf = sdf.filter(F.col('input_defhc_id')==self.defhc_id) \
                     .filter(F.col('radius')==self.radius) \
                     .filter(F.col('start_date')==self.start_date) \
                     .filter(F.col('end_date')==self.end_date) \
                     .filter(F.col('subset_lt18')==self.subset_lt18)

        distinct_return = sdf_check_distinct(sdf, cols)
        print(distinct_return)

        if 'NOT Distinct' in distinct_return:

            exit_notebook(f"ERROR: Duplicate records by {', '.join(cols)} in table '{name}'")
