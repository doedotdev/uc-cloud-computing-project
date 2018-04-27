# whether
Cloud Computing Final Project - Weather Data

## Method

I chose Pyspark with SQL to do my analysis. I felt comfortable with map reduce after the last assignment and wanted to 
try something new and learn a new skill set for utilization after graduation.


## Code Adaptation

Code adaptive from Professor's example shown below.

```
[tatavag@cscloud2017 ~]$ cat weather.py
import sys
from random import random
from operator import add
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
if __name__ == "__main__":
 """
 Usage: weather sourcefile
 """
 sc = SparkContext(appName="PythonPi")
 sqlContext = SQLContext(sc)
 file = sys.argv[1]

 lines = sc.textFile(file)
 parts = lines.map(lambda l: l.split(","))
 obs = parts.map(lambda p: Row(station=p[0], date=int(p[1]) , measurement=p[2] ,
value=p[3] ) )
 # Infer the schema, and register the DataFrame as a table.
 weather = sqlContext.createDataFrame(obs)
 weather.registerTempTable("weather")
 # SQL can be run over DataFrames that have been registered as a table.
 summary = sqlContext.sql("SELECT measurement, avg(value) value from weather
where measurement in ('TMIN', 'TMAX') and value <> 9999 group by measurement")
 summary.show()
 summary.rdd.map(lambda x: ",".join(map(str,
x))).coalesce(1).saveAsTextFile("hdfs:/user/tatavag/weather_results.csv")

 sc.stop()
[tatavag@cscloud2017 ~]$ hadoop fs -put weather.py /user/tatavag
[tatavag@cscloud2017 ~]$ spark-submit --deploy-mode cluster --master yarn
weather.py 'hdfs:/user/tatavag/weather/2000.csv'
OpenJDK Server VM warning: You have loaded library /tmp/libnetty-transport-nativeepoll2470421618618700239.so
which might have disabled stack guard. The VM will try to
fix the stack guard now.
It's highly recommended that you fix the library with 'execstack -c <libfile>', or
link it with '-z noexecstack'.
[tatavag@cscloud2017 ~]$ hadoop fs -cat /user/tatavag/weather_results.csv/*
TMAX,162.776329725
TMIN,45.4955559578
[tatavag@cscloud2017 ~]$
```

## Code Setup

All code snippets follow the same general structure.

- Loop through all the years
- look at the file related to that year
- rename the headers of the file
- create a temp table
- run a sql script on the temp table
    - exclude temp values of -9999 and 9999
    - Only look for QF values of:
        - ''
        - 'Z'
        - 'W'
    - Do not include SF values that blank
- Right out each year file to its respective location and name it based on the year.
    

 - for num in range(2000,2019):
        file = "hdfs:/user/tatavag/PIIweather/"+str(num)+".csv"
        lines = sc.textFile(file)
        parts = lines.map(lambda l: l.split(","))
        obs = parts.map(lambda p: Row(ID=p[0], DT=p[1], EL=p[2], VAL=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
        weatherDataFrame = sqlContext.createDataFrame(obs)
        weatherDataFrame.registerTempTable("weatherTable")
        query = sqlContext.sql("SELECT EL, avg(VAL) VAL from weatherTable WHERE EL in ('TMIN','TMAX') AND VAL <> 9999 AND VAL <> -9999 AND SF <> '' AND QF IN ('','Z','W') GROUP BY EL")
        query.show()
        query.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("hdfs:/user/hornbd/averageOut/avg" + str(num) + ".csv")


## Set Up

Data is located here:

```
hornbd@hadoop-gate-0:~$ hadoop fs -ls /user/tatavag/PIIweather
Found 19 items
-rwxr-xr-x   3 tatavag hdfs  202374348 2017-04-10 22:49 /user/tatavag/PIIweather/2000.csv
-rwxr-xr-x   3 tatavag hdfs  206452133 2017-04-10 22:49 /user/tatavag/PIIweather/2001.csv
-rwxr-xr-x   3 tatavag hdfs  212785919 2017-04-10 22:49 /user/tatavag/PIIweather/2002.csv
-rwxr-xr-x   3 tatavag hdfs  220036332 2017-04-10 22:49 /user/tatavag/PIIweather/2003.csv
-rwxr-xr-x   3 tatavag hdfs  224820780 2017-04-10 22:49 /user/tatavag/PIIweather/2004.csv
-rwxr-xr-x   3 tatavag hdfs  226447607 2017-04-10 22:49 /user/tatavag/PIIweather/2005.csv
-rwxr-xr-x   3 tatavag hdfs  227704442 2017-04-10 22:49 /user/tatavag/PIIweather/2006.csv
-rwxr-xr-x   3 tatavag hdfs  228913066 2017-04-10 22:49 /user/tatavag/PIIweather/2007.csv
-rwxr-xr-x   3 tatavag hdfs  228355930 2017-04-10 22:49 /user/tatavag/PIIweather/2008.csv
-rwxr-xr-x   3 tatavag hdfs  229678679 2017-04-10 22:49 /user/tatavag/PIIweather/2009.csv
-rwxr-xr-x   3 tatavag hdfs  230978566 2017-04-10 22:49 /user/tatavag/PIIweather/2010.csv
-rwxr-xr-x   3 tatavag hdfs  235110126 2017-04-10 22:49 /user/tatavag/PIIweather/2011.csv
-rwxr-xr-x   3 tatavag hdfs  232045738 2017-04-10 22:49 /user/tatavag/PIIweather/2012.csv
-rwxr-xr-x   3 tatavag hdfs  224334910 2017-04-10 22:49 /user/tatavag/PIIweather/2013.csv
-rwxr-xr-x   3 tatavag hdfs  220711466 2017-04-10 22:49 /user/tatavag/PIIweather/2014.csv
-rwxr-xr-x   3 tatavag hdfs  216443063 2017-04-10 22:49 /user/tatavag/PIIweather/2015.csv
-rwxr-xr-x   3 tatavag hdfs  214154079 2017-04-10 22:49 /user/tatavag/PIIweather/2016.csv
-rw-r--r--   3 tatavag hdfs  210607333 2018-04-11 22:56 /user/tatavag/PIIweather/2017.csv
-rw-r--r--   3 tatavag hdfs   51577244 2018-04-11 22:57 /user/tatavag/PIIweather/2018.csv
```

## Dataset as a dataframe
This is what the dataset as a dataframe looks like
```
+--------+----+-----------+---+---+---+----+
|      DT|ELEM|         ID| MF| QF| SF| VAL|
+--------+----+-----------+---+---+---+----+
|20000101|TMAX|USW00024229|   |   |  0|  78|
|20000101|TMIN|USW00024229|   |   |  0|  44|
|20000101|TMAX|USW00094626|   |   |  W| -28|
|20000101|TMIN|USW00094626|   |   |  W|-139|
|20000101|TMAX|USW00014719|   |   |  W|  78|
|20000101|TMIN|USW00014719|   |   |  W| -89|
|20000101|TMAX|USW00024061|   |   |  W|  39|
|20000101|TMIN|USW00024061|   |   |  W| -72|
|20000101|TMAX|USW00003967|   |   |  W| 172|
|20000101|TMIN|USW00003967|   |   |  W|  22|
|20000101|TMAX|USW00012842|   |   |  0| 250|
|20000101|TMIN|USW00012842|   |   |  0| 161|
|20000101|TMAX|USW00012876|   |   |  W| 256|
|20000101|TMIN|USW00012876|   |   |  W| 117|
|20000101|TMAX|USW00003889|   |   |  0| 139|
|20000101|TMIN|USW00003889|   |   |  0|  56|
|20000101|TMAX|USS0018F01S|   |   |  T| -24|
|20000101|TMIN|USS0018F01S|   |   |  T| -63|
|20000101|TMAX|USS0019L03S|   |   |  T|  52|
|20000101|TMIN|USS0019L03S|   |   |  T|-124|
+--------+----+-----------+---+---+---+----+
only showing top 20 rows
```

## Average TMIN, TMAX for each year excluding abnormalities or missing data

Average TMAX and TMIN for each year table:

| Year       | TMAX           | TMIN  |
| ------------- |-------------| -----|
| 2000      | 175.588635095 | 44.3074645256 |
| 2001      | 178.750696444 | 47.9236310463 |
| 2002 | 177.239736279      |  46.5584205402 |
| 2003 | 177.089492096      |  48.621655506 |
| 2004 | 174.972026256      |  49.5505342392 |
| 2005 | 177.906588295      |  49.9813615205  |
| 2006 | 181.091136655      |  51.1208690005 |
| 2007 | 178.438546747      |  49.2251569946 |
| 2008 | 170.386266962      |  40.9313710947 |
| 2009 | 169.156740108      |  43.7015996749 |
| 2010 | 171.776155528      |  47.4770741976 |
| 2011 | 172.772699479      |  46.2956648155 |
| 2012 | 184.473303518      |  53.8694278478 |
| 2013 | 168.030526905      |  43.5392789655 |
| 2014 | 169.242773864      | 44.3211652736 |
| 2015 | 178.05388895       | 54.0385015779 |
| 2016 | 179.872150083      | 55.5844555765 |
| 2017 | 176.564373112      | 52.6911048638 |
| 2018 | 78.8660785863      | -37.3588068434 |

All the output files can be found here.

```
hornbd@hadoop-gate-0:~$ hadoop fs -ls averageOut
Found 19 items
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:37 averageOut/avg2000.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:39 averageOut/avg2001.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:40 averageOut/avg2002.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:42 averageOut/avg2003.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:44 averageOut/avg2004.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:45 averageOut/avg2005.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:47 averageOut/avg2006.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:49 averageOut/avg2007.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:51 averageOut/avg2008.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:52 averageOut/avg2009.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:54 averageOut/avg2010.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:56 averageOut/avg2011.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:58 averageOut/avg2012.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 17:59 averageOut/avg2013.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:02 averageOut/avg2014.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:04 averageOut/avg2015.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:06 averageOut/avg2016.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:07 averageOut/avg2017.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:23 averageOut/avg2018.csv
```

And they all look very similar this.

```
hornbd@hadoop-gate-0:~$ hadoop fs -cat averageOut/avg2018.csv/part-00000
TMAX,78.8660785863
TMIN,-37.3588068434
hornbd@hadoop-gate-0:~$ `

```

The code for this section is as follows.

```
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
```

## Minimum TMIN for each year excluding abnormalities or missing data

Min TMin for each year table:

| Year       | TEMP       |
| ------------- |----------
| 2000      | -578 |
| 2001      | -528 |
| 2002      | -472 |
| 2003      | -500 |
| 2004      | -533 |
| 2005      | -550 |
| 2006      | -528 |
| 2007      | -539 |
| 2008      | -578 |
| 2009      | -556 |
| 2010      | -533 |
| 2011      | -517 |
| 2012      | -544 |
| 2013      | -528 |
| 2014      | -500 |
| 2015      | -528 |
| 2016      | -469 |
| 2017      | -520 |
| 2018      | -528 |

All the output files can be found here.

```
hornbd@hadoop-gate-0:~$ hadoop fs -ls /user/hornbd/coldOut
Found 19 items
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:50 /user/hornbd/coldOut/cold2000.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:51 /user/hornbd/coldOut/cold2001.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:52 /user/hornbd/coldOut/cold2002.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:54 /user/hornbd/coldOut/cold2003.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:55 /user/hornbd/coldOut/cold2004.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:00 /user/hornbd/coldOut/cold2005.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:03 /user/hornbd/coldOut/cold2006.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:04 /user/hornbd/coldOut/cold2007.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:06 /user/hornbd/coldOut/cold2008.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:08 /user/hornbd/coldOut/cold2009.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:10 /user/hornbd/coldOut/cold2010.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:12 /user/hornbd/coldOut/cold2011.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:14 /user/hornbd/coldOut/cold2012.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:16 /user/hornbd/coldOut/cold2013.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:18 /user/hornbd/coldOut/cold2014.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:20 /user/hornbd/coldOut/cold2015.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:22 /user/hornbd/coldOut/cold2016.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:24 /user/hornbd/coldOut/cold2017.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:24 /user/hornbd/coldOut/cold2018.csv
```

And they all look very similar this.

```
hornbd@hadoop-gate-0:~$ hadoop fs -cat /user/hornbd/coldOut/cold2018.csv/part-00000
-528
hornbd@hadoop-gate-0:~$ 

```

The code for this section is as follows.

```
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
```


## Maximum TMAX for each year excluding abnormalities or missing data

Max TMax for each year table:

| Year       | TEMP       |
| ------------- |----------
| 2000 |  522 |
| 2001 |  528 |
| 2002 |  533 |
| 2003 |  533 |
| 2004 |  517 |
| 2005 |  539 |
| 2006 |  528 |
| 2007 |  556 |
| 2008 |  528 |
| 2009 |  533 |
| 2010 |  517 |
| 2011 |  511 |
| 2012 |  537 |
| 2013 |  539 |
| 2014 |  522 |
| 2015 |  556 |
| 2016 |  539 |
| 2017 |  528 |
| 2018 |  400 |


All the output files can be found here.

```
hornbd@hadoop-gate-0:~$ hadoop fs -ls /user/hornbd/hotOut
Found 19 items
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 18:58 /user/hornbd/hotOut/hot2000.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:02 /user/hornbd/hotOut/hot2001.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:03 /user/hornbd/hotOut/hot2002.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:05 /user/hornbd/hotOut/hot2003.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:07 /user/hornbd/hotOut/hot2004.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:09 /user/hornbd/hotOut/hot2005.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:11 /user/hornbd/hotOut/hot2006.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:13 /user/hornbd/hotOut/hot2007.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:15 /user/hornbd/hotOut/hot2008.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:16 /user/hornbd/hotOut/hot2009.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:19 /user/hornbd/hotOut/hot2010.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:21 /user/hornbd/hotOut/hot2011.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:23 /user/hornbd/hotOut/hot2012.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:25 /user/hornbd/hotOut/hot2013.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:26 /user/hornbd/hotOut/hot2014.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:32 /user/hornbd/hotOut/hot2015.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:34 /user/hornbd/hotOut/hot2016.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:40 /user/hornbd/hotOut/hot2017.csv
drwxr-xr-x   - hornbd hdfs          0 2018-04-26 19:41 /user/hornbd/hotOut/hot2018.csv
```

And they all look very similar this.

```
hornbd@hadoop-gate-0:~$ hadoop fs -cat /user/hornbd/hotOut/hot2018.csv/part-00000
400

```

The code for this section is as follows.

```
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

```

## 5 hottest weather stations for each year excluding abnormalities or missing data

| Year       | ONE       | TWO | THREE | FOUR | FIVE |
| ------------- |----------| ---- | ----- | ----- | -----| 
| 2000 |  USW00022501 | USW00022519 | USW00093831 | USW00021510 | USC00515177 | 
| 2001 |  USR0000COAM | USC00395691 | USC00514400 | USC00515710 | USR0000HHON | 
| 2002 |  USC00347254 | USC00313238 | USC00517781 | USW00021510 | USW00022521 | 
| 2003 |  USR0000CWIL | USC00344766 | USC00517781 | USW00021510 | USC00515864 | 
| 2004 |  USC00042346 | USC00513208 | USC00138808 | USC00513911 | USC00514400 | 
| 2005 |  USR0000MKIL | USC00383906 | USC00290525 | USC00311515 | USC00016000 | 
| 2006 |  USC00080611 | USC00021514 | USC00087189 | USC00236775 | USR0000CNOV | 
| 2007 |  USC00314987 | USC00412082 | USC00344384 | USC00511918 | USW00021510 | 
| 2008 |  USC00411671 | USC00418715 | USC00230820 | USC00254113 | USR0000MWIL | 
| 2009 |  USC00141408 | USC00144530 | USC00083137 | USC00515286 | USC00415596 | 
| 2010 |  USC00141408 | USC00343835 | USC00153246 | USC00517948 | USC00503212 | 
| 2011 |  USR0000CTAR | USC00035514 | USC00412350 | USC00141408 | USC00364611 | 
| 2012 |  USC00290915 | USC00415101 | USC00047404 | USC00513911 | USW00021510 | 
| 2013 |  USC00014798 | USC00410204 | USC00165875 | USW00021510 | USR0000HHON | 
| 2014 |  USC00234377 | USC00156732 | USC00396170 | USC00413618 | USC00518600 | 
| 2015 |  USC00403379 | USC00092593 | USC00047836 | USC00294862 | USC00517150 | 
| 2016 |  USC00415101 | USC00222896 | USC00312940 | USC00412906 | USC00419605 | 
| 2017 |  USC00415101 | USC00265869 | USC00094429 | USW00013702 | USW00013807 | 
| 2018 |  USC00415721 | USC00518422 | USC00515758 | USC00513117 | USC00513977 |


```
hornbd@hadoop-gate-0:~$ hadoop fs -cat /user/hornbd/hotStation/hot2000.csv/part-00000
USW00022501
USW00022519
USW00093831
USW00021510
USC00515177
```

Code for this section:

```
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

```

## 5 coldest weather stations for each year excluding abnormalities or missing data

| Year       | ONE       | TWO | THREE | FOUR | FIVE |
| ------------- |----------| ---- | ----- | ----- | -----| 
| 2000 |   USC00501684 | USC00505644 | USC00508140 | USW00026443 | USC00509313 | 
| 2001 |   USW00026508 | USR0000ABCA | USS0051R01S | USC00501492 | USR0000ABEV | 
| 2002 |   USR0000AKAI | USS0050S01S | USS0051R01S | USR0000ABEV | USC00503212 |
| 2003 |   USC00501492 | USS0051R01S | USW00026533 | USS0050S01S | USC00509869 | 
| 2004 |   USC00501684 | USC00502568 | USC00502350 | USW00026412 | USS0045O10S | 
| 2005 |   USC00501684 | USC00509313 | USC00509869 | USC00502568 | USW00026412 | 
| 2006 |   USR0000ASEL | USC00501492 | USR0000ABEV | USR0000ACHL | USC00501684 | 
| 2007 |   USC00501684 | USS0045R01S | USW00026422 | USR0000ACHL | USC00502607 | 
| 2008 |   USC00501684 | USC00501492 | USR0000ACHL | USS0045R01S | USW00026412 | 
| 2009 |   USC00502101 | USC00501684 | USC00509313 | USR0000ACHL | USR0000ABEV | 
| 2010 |   USC00501684 | USC00502101 | USS0051R01S | USR0000ACHL | USS0045R01S | 
| 2011 |   USC00509869 | USS0045R01S | USS0051R01S | USC00501684 | USC00501492 | 
| 2012 |   USC00503165 | USC00503212 | USS0051R01S | USC00504210 | USS0045R01S | 
| 2013 |   USC00502339 | USC00501684 | USS0051R01S | USS0045R01S | USR0000ABCK | 
| 2014 |   USC00501684 | USS0045R01S | USR0000ABCK | USS0048V01S | USS0051R01S | 
| 2015 |   USC00502339 | USC00501684 | USS0041P07S | USC00509314 | USW00026533 | 
| 2016 |   USS0051R01S | USC00501684 | USR0000ACHL | USS0045R01S | USS0041P07S | 
| 2017 |   USS0051R01S | USW00026529 | USR0000ASLC | USR0000ALIV | USS0045O04S | 
| 2018 |   USC00502339 | USR0000ANOR | USC00501684 | USR0000AKAV | USW00096406 |
```
hornbd@hadoop-gate-0:~$ hadoop fs -cat /user/hornbd/coldStation/cold2000.csv/part-00000
USC00501684
USC00505644
USC00508140
USW00026440
USC00509313

```

Code for this section:
```
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
```



