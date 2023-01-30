# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include all geo functions for provider dashboard

# COMMAND ----------

import geopandas as gpd 

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

def get_coordinates(df, longitude='longitude', latitude='latitude'):
    """
    Function get_coordinates to take input pandas df with longitude and latitude columns and return
        coordinates project to crs using meters
        
    params:
        df pandas df: df with lat/long cols
        longitude str: optional param to specify column with longitude value, default='longitude'
        latitude str: optional param to specify column with latitude value, default='latitude'
        
    returns:
        GeoDataFrame with reprojected coordinates
    
    """

    gdf = gpd.GeoDataFrame(df
                           , geometry=gpd.points_from_xy(df[longitude], df[latitude])
                           , crs = 'epsg:4326')

    # Now reproject to a crs using meters
    return gdf.to_crs('epsg:3857')

# COMMAND ----------

def get_intersection(base_gdf, match_gdf, id_col, keep_coords='None', **kwargs):
    """
    Function get_intersection to take in base GeoDataFrame and get all intersecting coordinates from match_gdf,
        to return spark df with specified schema retaining integer id_col and string zipcode
        
    params:
        base_gdf GeoDataFrame: base geo df
        match_gdf GeoDataFrame: matching geo df to find intersection against base
        id_col str: name of id col to keep
        keep_coords string: optional param to specify whether to keep lat/long from either df, default=None, otherwise = 'left' or 'right' (case-insensitive)
        **kwargs: optional params, current implemented functionalities are keep_cols and keep_types, with lists of additional cols to keep with types (must be on one df only)
        
    returns:
        spark df with intersection, keeping id_col and zip, lat/long if requested
    
    
    """

    intersection = gpd.sjoin(base_gdf, match_gdf, how='left')
    
    schema = StructType([ StructField(id_col, IntegerType(), True), StructField("zip", StringType(), True)])
    keep_cols = [id_col, "zip"]
    
    keep_coords = keep_coords.lower()
    if keep_coords in ['left','right']:
        
        # if coords specified to keep, rename on merged df and add to schema and list of keep_cols
        intersection.rename(columns = {f"longitude_{keep_coords}": 'longitude', f"latitude_{keep_coords}": 'latitude'}, inplace=True)
        
        schema_coords = StructType([ StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True)])
        schema = StructType([schema.fields + schema_coords.fields][0])
        
        keep_cols += ['latitude', 'longitude']
        
    if kwargs.get('keep_cols', None):
        schema_addtl = StructType([ StructField(msr_name, msr_type, False) for msr_name, msr_type in zip(kwargs['keep_cols'], kwargs['keep_types'])])
        schema = StructType([schema.fields + schema_addtl.fields][0])
        
        keep_cols += kwargs['keep_cols']
        
    
    return spark.createDataFrame(intersection[keep_cols], schema=schema)
