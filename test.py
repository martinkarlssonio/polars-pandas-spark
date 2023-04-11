#SPARK
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
#PANDAS
import pandas as pd
#POLARS
import polars as pl
#OTHERS
import random
import time
import os
import logging

#Configuration
testType = str(os.getenv('TEST_TYPE', default="polars"))
dataframeN = int(os.getenv('DATAFRAME_N', default="10000"))
calcN = int(os.getenv('CALC_N', default="1000"))

def mockedDict():
    #Generate a list of unique ID's
    uniqueIdList = []
    for n in range(1,50):
        uniqueIdList.append("machine"+str(n))
    #Generate mocked data
    idList = []
    valueAList = []
    valueBList = []
    valueCList = []
    valueDList = []
    for n in range(0,dataframeN):
        idList.append(random.choice(uniqueIdList))
        valueAList.append(round(random.uniform(40,100), 2))
        valueBList.append(round(random.uniform(40,100), 2))
        valueCList.append(round(random.uniform(40,100), 2))
        valueDList.append(round(random.uniform(40,100), 2))
        
    data = {
        "id":idList,
        "valueA":valueAList,
        "valueB":valueBList,
        "valueC":valueCList,
        "valueD":valueDList
    }
    return data

#Fetch mocked data
dataDict = mockedDict()
#Create a Pandas Dataframe
pandasDf = pd.DataFrame(dataDict)
#Create a Polars Dataframe
polarsDf = pl.DataFrame(dataDict)
#Create a Spark dataframe
dataList = [Row(**{k: v[i] for k, v in dataDict.items()}) for i in range(len(dataDict["id"]))]
sparkDf = spark.createDataFrame(dataList)
#sparkDf = spark.createDataFrame(pandasDf)

#Pandas single core by default
def pandasTest(testDf):
    pandasStartEpoch = int(time.time()*1000)
    idListPandas = list(testDf['id'].unique())
    for n in range(0,calcN):
        groupedPandasDf = testDf.groupby("id").sum()
    pandasEndEpoch = int(time.time()*1000)
    pandasTimeSpent = float((pandasEndEpoch-pandasStartEpoch)/1000)
    return pandasTimeSpent

#Polars multi core by default
def polarsTest(testDf):
    polarsStartEpoch = int(time.time()*1000)
    idListPolars = list(testDf['id'].unique())
    for n in range(0,calcN):
        groupedPolarsDf = testDf.groupby("id").sum()
    polarsEndEpoch = int(time.time()*1000)
    polarsTimeSpent = float((polarsEndEpoch-polarsStartEpoch)/1000)
    return polarsTimeSpent

#Spark multi core by default
def sparkTest(testDf):
    sparkStartEpoch = int(time.time()*1000)
    idListSpark = testDf.select("id").rdd.flatMap(lambda x: x).collect()
    for n in range(0,calcN):
        groupedSparkDf = testDf.groupBy("id").sum()
    sparkEndEpoch = int(time.time()*1000)
    sparkTimeSpent = float((sparkEndEpoch-sparkStartEpoch)/1000)
    return sparkTimeSpent


if __name__ == "__main__":
    ##PANDAS
    if testType == "pandas":
        logging.info("##################### START WITH PANDAS")
        time.sleep(10)
        pandasTimeSpent = pandasTest(pandasDf)
        logging.info("##################### Pandas iteration took : {}s".format(pandasTimeSpent))
        logging.info(" ")

    ##POLARS
    if testType == "polars":
        logging.info("##################### START WITH POLARS")
        time.sleep(10)
        polarsTimeSpent = polarsTest(polarsDf)
        logging.info("##################### Polars iteration took : {}s".format(polarsTimeSpent))
        logging.info(" ")

    ##SPARK
    if testType == "spark":
        logging.info("##################### START WITH SPARK")
        time.sleep(10)
        sparkTimeSpent = sparkTest(sparkDf)
        logging.info("##################### Spark iteration took : {}s".format(sparkTimeSpent))
