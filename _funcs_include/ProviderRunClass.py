# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook with main provider class

# COMMAND ----------

# MAGIC %run ./params

# COMMAND ----------

# MAGIC %run ./credentials

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/fs_funcs

# COMMAND ----------

# MAGIC %run /Repos/Data_Science/general_db_funcs/_general_funcs/sdf_funcs

# COMMAND ----------

# MAGIC %run ./output_funcs

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

class ProviderRun(object):
    
    def __init__(self, defhc_id, radius, start_date, end_date, subset_lt18, database, run_qc,
                 charts_instance = True,
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
        self.charts_instance = charts_instance
        self.base_output_prefix = base_output_prefix
        self.db_tables = db_tables
        self.counts_table = f"{self.database}.{counts_table}"
        self.status_table = f"{self.database}.{status_table}"
        
        # create name for facility-specific database
        self.fac_database = f"{self.database}_{self.defhc_id}"
        
        # create base output table to join all other outputs to
        self.base_output_table = self.base_output_table()
        
        # create empty dict to save table counts
        self.table_counts = {}
        
        # if charts instance of class, create views and set network and name attributes
        if self.charts_instance:
            
            self.create_views()
            
            self.input_network, self.defhc_name = sdf_return_row_values(hive_to_df('input_org_info_vw'), ['input_network', 'defhc_name'])
        
    def condition_stmt(self, **kwargs):
        """
        method condition_stmt to return text string (without 'where') with conditional
            subset to all input base params
            
        returns:
            string with condition statement
        
        """
        
        id_prefix = kwargs.get('id_prefix', self.base_output_prefix)
        
        return f""" {id_prefix}defhc_id = {self.defhc_id} and 
                    radius = {self.radius} and 
                    start_date = '{self.start_date}' and 
                    end_date = '{self.end_date}' and
                    subset_lt18 = {self.subset_lt18}
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
    
    def insert_table_counts(self, database, table_name, count):
        """
        method insert_table_counts to insert record into self.counts_table for given db table
        params:
            database str: name of database to insert
            table_name str: name of table to insert
            count int: count to insert
            
        returns:
            none
        
        """

        # create sdf with base cols, count and time stamp, then call populate_counts() to set any remaining recs to False and insert new rec

        counts_sdf = self.create_final_output(counts_sdf = spark.createDataFrame(pd.DataFrame(data={'database': database,
                                                                                                    'table_name': table_name,
                                                                                                    'count': count}, 
                                                                                              index=[0])),
                                              insert = False
                                             )

        # on counts_sdf, if self.base_output_prefix is not empty, must REMOVE id_prefix (no prefix on most recent table)
        
        if self.base_output_prefix != '':
            counts_sdf = counts_sdf.withColumnRenamed(f"{self.base_output_prefix}defhc_id", 'defhc_id')

        populate_most_recent(sdf = counts_sdf,
                             table = self.counts_table,
                             condition = f"{self.condition_stmt(id_prefix='')} and database = '{database}' and table_name = '{table_name}'")

    def insert_into_output(self, sdf, table, must_exist=True, maxrecs=25):
        """
        method insert_into_output() to insert new data into table, 
            first checking if there are records existing for given id/radius/dates, and if so, deleting before insertion
            
            will upload to s3 is self.charts_instance
            will add record count to self.counts_table

        params:
            sdf spark df: spark df with records to be inserted (will insert all rows, all columns)
            table str: name of output table 
            must_exist bool: optional param to specify table must exist before run, default = True
                if not, will create if does NOT exist and will print message that created
            maxrecs int: optional param to specify max recs to display, default = 25

        returns:
            none (prints sample recs/counts deleted and/or added)

        """
        
        # create view from sdf

        sdf.createOrReplaceTempView('sdf_vw')

        # only run commands to select/delete from given table IF table must exist OR must not exist but already exists

        database, table_name = table.split('.')[0], table.split('.')[1]

        table_exists = spark._jsparkSession.catalog().tableExists(database, table_name)

        if must_exist or (must_exist==False and table_exists):

            old_dts = spark.sql(f"""
                select distinct current_dt
                from {table}
                where {self.condition_stmt()}
                """).collect()

            if len(old_dts) > 0:
                print(f"Old records found, will delete: {', '.join([str(dt[0]) for dt in old_dts])}")
            else:
                print("No old records found to delete")

            spark.sql(f"""
                delete
                from {table}
                where {self.condition_stmt()}
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
           where {self.condition_stmt()}
           limit {maxrecs}
           """).display()
        
        # upload to s3 if charts instance of class
        
        if self.charts_instance:
            csv_upload_s3(table, bucket=S3_BUCKET, key_prefix=S3_KEY, **AWS_CREDS)
            
        tbl_count = hive_tbl_count(table, condition = f"where {self.condition_stmt()}")
        
        # update table counts
        self.table_counts[table] = tbl_count
        
        # populate table counts
        
        self.insert_table_counts(database = database, table_name = table_name, count = tbl_count)
        
    def create_final_output(self, counts_sdf, insert=True, **kwargs):
        """
        method create_final_output to read in base and counts spark dfs to add base columns to counts, and add final time stamp
        params:
            counts_sdf sdf: sdf with all counts
            insert bool: optional param to specify whether to ADDITIONALLY call self.insert_into_output() to insert final table, default = True
            **kwargs: kwargs to pass to self.insert_into_output(), if insert=True MUST pass 'table' as kwarg

        returns:
            none if insert=True (will call self.insert_into_output() after creation of final table),
                otherwise returns spark df with base_sdf cols, counts_sdf cols, and current_dt
        """

        final_sdf = self.base_output_table.join(counts_sdf).withColumn('current_dt', F.current_timestamp())
        
        if insert:
            self.insert_into_output(final_sdf, kwargs.get('table'), kwargs.pop('table'))
            
        else:
            return final_sdf
        
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
                from {self.fac_database}.{tbl}
                where {self.condition_stmt(id_prefix=id_prefix)}
            """)
            cnt = hive_tbl_count(f"{tbl}_vw")

            assert cnt >0, f"ERROR: table {self.fac_database}.{table} has 0 records for given id/radius/time frame"

            print(f"{tbl}_vw created with {cnt:,d} records")
            
    def insert_run_status(self, success=True, fail_message=None):
        """
        method insert_run_status to call at end of given run to fill with run info, run number, and success/fail message
        
        params:
            success bool: optional param to set success col, default = True
            fail_message str: optional param to insert fail message if success = False, default = None (will fill with Null)
            
        returns:
            print of all records from status table for given params
        
        """
        
        run_sdf = self.create_final_output(counts_sdf = spark.createDataFrame(pd.DataFrame(data={'success': success,
                                                                                                 'fail_reason': fail_message}, 
                                                                                              index=[0])),
                                              insert = False
                                             )

        if self.base_output_prefix != '':
            run_sdf = run_sdf.withColumnRenamed(f"{self.base_output_prefix}defhc_id", 'defhc_id')
        
        populate_most_recent(sdf = run_sdf,
                             table = self.status_table,
                             condition = self.condition_stmt(id_prefix=''))
        
        print(f"All recs for given params:")

        spark.sql(f"""
           select *
           from {self.status_table}
           where {self.condition_stmt(id_prefix='')}
           order by run_number
           """).display()
        
    def exit_notebook(self, fail_message=None, final_nb=False):
        """
        method exit_notebook to use to pass fail messages/params between notebooks, AND
            if fail or end of process, update run status table

        params:
            fail_message str: optional param if want to pass fail message, if kept as None means success
            final_nb bool: optional param to specify final notebook in run (so update run status table), default = False

        returns:
            dbutils.notebook.exit() with dictionary of return params

        """

        # if fail, return fail message AND update run status table with same message

        if fail_message is not None:
            notebook_return = {'message': fail_message,
                              'fail': True}
            
            self.insert_run_status(success=False, fail_message=fail_message)

        else:
            notebook_return = {'message': self.table_counts}

            notebook_return['fail'] = False
            
            # if final notebook, update run status table (set success=True)
            if final_nb:
                self.insert_run_status()
                
        return dbutils.notebook.exit(notebook_return)
        
    def test_distinct(self, sdf, name, cols, to_subset=True):
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

            self.exit_notebook(fail_message = f"ERROR: Duplicate records by {', '.join(cols)} in table '{name}'")
