"""
Main Spark Application
"""
from math import radians, cos, sin, asin, sqrt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from pyspark.sql.functions import mean, stddev
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
#========================================

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

#========================================

conf = SparkConf().setAppName("insight")
sc = SparkContext(conf=conf)
distance = udf(haversine, FloatType())
spark = SparkSession.builder.getOrCreate()

# sc.setLogLevel("ERROR")
sc.setLogLevel("OFF")

POIList = spark.read.option("inferSchema", "true").option("header", "true").csv("POIList.csv")
SampleData = spark.read.option("inferSchema", "true").option("header", "true").csv("DataSample.csv")

# Column name clean-up
POIList = POIList.selectExpr("` Latitude` as POILatitude", "Longitude as POILongitude", "POIID as POIID")
SampleData = SampleData.withColumnRenamed(" TimeSt", "TimeSt")


SampleData.createOrReplaceTempView("sampleDF")
SampleData.cache()
POIList.cache()

badData = spark.sql("""
                    SELECT sampleDF._ID
                    FROM sampleDF, sampleDF as SD
                    WHERE sampleDF._ID != SD._ID AND 
                    sampleDF.TimeSt = SD.TimeSt AND 
                    sampleDF.Latitude = SD.Latitude AND 
                    sampleDF.Longitude = SD.Longitude
                """)

badData = badData.withColumnRenamed("_ID", "ID")
badData.createOrReplaceTempView("bad_data")

cleanData = spark.sql("""
                      SELECT *
                      FROM sampleDF
                      LEFT OUTER JOIN bad_data ON bad_data.ID = sampleDF._ID 
                      WHERE bad_data.ID IS NULL                 
                    """)

cleanData = cleanData.drop(cleanData.ID)
cleanData.createOrReplaceTempView("clean_data")

# SANITY CHECK (for testing purposes)
#===============================================

# sanityCheck = spark.sql('''
#                         SELECT * 
#                         FROM clean_data 
#                         INNER JOIN bad_data
#                         WHERE clean_data._ID = bad_data.ID
#                     ''')

# Must have zero rows
# print("=======///////======")
# sanityCheck.show()
# print("=======///////======")

# test = spark.sql(""" 
#                     SELECT * FROM clean_data WHERE _ID = '4517905'
#                 """)
# test.show()
#===============================================

# Aggrigate SampleData with POIList
POIList.createOrReplaceTempView("POI_list")

# clean-up the POIList due to redundant coordinates for two POIs 
POIList = spark.sql(""" 
                    SELECT * 
                    FROM POI_list
                    WHERE POIID <> 'POI2'
                 """)
                 
POIList.createOrReplaceTempView("POI_list")
dataJoinedWithPOIList = spark.sql("""
                                SELECT * FROM clean_data CROSS JOIN POI_list
                              """)

# Apply the Haversine function to each row to get the distance 
dataWithDistance = dataJoinedWithPOIList.withColumn("distance", distance(dataJoinedWithPOIList.Latitude, dataJoinedWithPOIList.Longitude, dataJoinedWithPOIList.POILatitude, dataJoinedWithPOIList.POILongitude))

#Apply Min function on SampleData and only select the minimum distances
dataWithMinDistance = dataWithDistance.groupBy("_ID").min("distance").withColumnRenamed("min(distance)", "minimum_distance")
dataWithMinDistance = dataWithMinDistance.withColumnRenamed("_ID", "ID")


dataWithDistance.createOrReplaceTempView("data_with_distance")
dataWithMinDistance.createOrReplaceTempView("data_with_min_distance")

# Filter out data with appropriate label
lastDF = spark.sql("""
                    SELECT dwd._ID, dwd.TimeSt, dwd.Country, dwd.Province, dwd.City, dwd.Latitude, dwd.Longitude, dwd.POILatitude, dwd.POILongitude, dwd.POIID, dwd.distance
                    FROM data_with_distance AS dwd
                    JOIN data_with_min_distance AS dwmd
                    ON dwd.distance = dwmd.minimum_distance
                    AND dwd._ID = dwmd.ID
                """)

lastDF.createOrReplaceTempView('lastDF')
locationsList = POIList.select('POIID').toPandas()
lastDF.cache()

# Group by POID
grouped = lastDF.groupBy('POIID')

# Mean for POI Locations
meu = grouped.mean('distance').withColumnRenamed('avg(distance)', 'POImean')

# Standard deviation for POI Locationss
sigma = spark.sql(
    """
    SELECT POIID, stddev(distance) as stdd FROM lastDF GROUP BY POIID
    """
)

# Radius 
radius = grouped.max('distance').withColumnRenamed('max(distance)', 'radius')

# Counts of each POIID 
counts = grouped.count()

# Convert to Pandas DataFrame
radius = radius.toPandas()
counts = counts.toPandas()

# Compute the densities
densities = counts['count'] / radius['radius']

# scaled dencity
scaled = np.interp(densities, (densities.min(), densities.max()), (-10, +10))

data = pd.DataFrame(scaled)

plt.bar(data[0])
plt.savefig('test.pdf')