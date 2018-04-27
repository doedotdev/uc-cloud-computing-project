import sys
from random import random
from operator import add
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext

# SF <> '' => Blank = No source (i.e., data value missing)
# VALUE 9999 = Value Missing
# VALUE -9999 = missing value
# QF Flags that I will accept in analysis - really any that did not failm so blank, '', 'Z', and 'W'.

# Get Average Max and Average Min of each year.
def runAverage():
    for num in range(2000,2019):
        file = "hdfs:/user/tatavag/PIIweather/"+str(num)+".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        query = sqlContext.sql("SELECT EL, avg(VAL) VAL from weatherTable WHERE EL in ('TMIN','TMAX') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY EL")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/averageOut/avg" + str(num) + ".csv")
    sc.stop()

def runColdest():
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/"+str(num)+".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # just get single coldest
        query = sqlContext.sql("SELECT VAL from weatherTable WHERE EL in ('TMIN') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY VAL ORDER BY VAL ASC LIMIT 1")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/coldOut/cold" + str(num) + ".csv")
    sc.stop()

def runHotest():
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/" + str(num) + ".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # get single hottest date by ordering same as coldest but descending
        query = sqlContext.sql("SELECT VAL from weatherTable WHERE EL in ('TMAX') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY VAL ORDER BY VAL DESC LIMIT 1")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/hotOut/hot" + str(num) + ".csv")
    sc.stop()

### same as hot stations just looking for TMIN and sorting by ASC
def coldStations():
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/" + str(num) + ".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # select distinct incase a station shows up more than once, show the next coldest
        query = sqlContext.sql("SELECT DISTINCT ID, MIN(VAL) as STATIONTEMP from weatherTable WHERE EL='TMIN' AND VAL <> 9999 AND VAL <> -9999  AND SF <> '' AND QF IN ('','Z','W') GROUP BY ID ORDER BY STATIONTEMP ASC LIMIT 5")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/coldStation/cold" + str(num) + ".csv")
    sc.stop()


### same as cold stations just looking for TMAX and sort DESC
def hotStations():
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/" + str(num) + ".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # select distinct incase a station shows up more than once, show the next coldest
        query = sqlContext.sql("SELECT DISTINCT ID, MIN(VAL) as STATIONTEMP from weatherTable WHERE EL='TMAX' AND VAL <> 9999 AND VAL <> -9999  AND SF <> '' AND QF IN ('','Z','W') GROUP BY ID ORDER BY STATIONTEMP DESC LIMIT 5")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/hotStation/hot" + str(num) + ".csv")
    sc.stop()

def hottestEver():
    tempMaxValue = 0
    tempMaxDf = 0
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/" + str(num) + ".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # select distinct incase a station shows up more than once, show the next coldest
        query = sqlContext.sql("SELECT ID, DT, VAL from weatherTable WHERE EL in ('TMAX') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY ID, DT, VAL ORDER BY VAL DESC LIMIT 1")
        query.show()
        temp = query.select('VAL').collect()[0][0]
        if temp > tempMaxValue:
            tempMaxDf = query
            tempMaxDf.show()
            tempMaxValue = temp
    print("FINAL")
    tempMaxDf.show()
    sc.stop()



def coldestEver():
    tempMinValue = 0
    tempMinDf = 0
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/" + str(num) + ".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # select distinct incase a station shows up more than once, show the next coldest
        query = sqlContext.sql("SELECT ID, DT, VAL from weatherTable WHERE EL in ('TMIN') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY ID, DT, VAL ORDER BY VAL ASC LIMIT 1")
        query.show()
        temp = query.select('VAL').collect()[0][0]
        if temp < tempMinValue:
            tempMinDf = query
            tempMinDf.show()
            tempMinValue = temp
    print("FINAL")
    tempMinDf.show()
    sc.stop()


def runHotMed():
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/" + str(num) + ".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # get single hottest date by ordering same as coldest but descending
        query = sqlContext.sql("SELECT ID, percentile_approx(VAL, 0.5) from weatherTable WHERE EL in ('TMAX') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY ID")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/hotOutMed/medHot" + str(num) + ".csv")
    sc.stop()


def runColdMed():
    for num in range(2000, 2019):
        file = "hdfs:/user/tatavag/PIIweather/" + str(num) + ".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        lables = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(lables)
        weatherDataFrame.registerTempTable("weatherTable")
        # get single hottest date by ordering same as coldest but descending
        query = sqlContext.sql("SELECT percentile_approx(VAL, 0.5) from weatherTable WHERE EL in ('TMAX') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY VAL ORDER BY VAL")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/hotOutMed/medHot" + str(num) + ".csv")
    sc.stop()


