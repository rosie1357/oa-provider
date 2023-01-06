# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Notebook to include all geo functions for provider dashboard

# COMMAND ----------

import geopandas as gpd 

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

def get_intersection(base_gdf, match_gdf, id_col):
    """
    Function get_intersection to take in base GeoDataFrame and get all intersecting coordinates from match_gdf,
        to return spark df with specified schema retaining integer id_col and string zipcode
        
    params:
        base_gdf GeoDataFrame: base geo df
        match_gdf GeoDataFrame: matching geo df to find intersection against base
        id_col str: name of id col to keep
        
    returns:
        spark df with intersection, keeping id_col and zip
    
    
    """

    intersection = gpd.sjoin(base_gdf, match_gdf, how='left')
    
    schema = StructType([ StructField(id_col, IntegerType(), True), StructField("zip", StringType(), True)])

    return spark.createDataFrame(intersection[[id_col, "zip"]], schema=schema)
