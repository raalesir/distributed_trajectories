"""
Constants for the package
=========================
"""
from  pyspark.sql import  SparkSession


beijing_lat_box = [39.6, 40.2]
beijing_lon_box = [116.1, 116.7]
width = 1
n = lon_cells = 100
m = lat_cells = 100
OD_time_frame = 2*60*60
delta_avg_ts = 400
speed = 0.0005 # graduses/ second...
timestamps_per_hour = 6




spark = SparkSession.builder\
    .enableHiveSupport()\
    .appName('Distributed Trajectories') \
    .master("local[*]")\
    .getOrCreate()