import sys
import csv
#import org.apache.log4j.{Level, Logger}                                                                                                                                                                    
from pyspark import SparkContext, SparkConf
from datetime import datetime
from operator import add, itemgetter
from collections import namedtuple
from datetime import datetime
import os
import time
from StringIO import StringIO

#Defining the fields, Creating a Flights class with the following fields as a tuple                                                                                                                         
#Each row is converted into a list                                                                                                                                                                          

timestarted = time.time()

fields   = ('date', 'airline', 'flightnum', 'origin', 'dest', 'dep',
            'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance')

Flight   = namedtuple('Flight', fields, verbose=True)

DATE_FMT = "%Y-%m-%d"

TIME_FMT = "%H%M"

# User Defined Functions                                                                                                                                                                                    

def toCSVLine(data):
  return ','.join(str(d) for d in data)


def split(line):
    reader = csv.reader(StringIO(line))
    return reader.next()

def parse(row):
    row[0]  = datetime.strptime(row[0], DATE_FMT).date()
    row[5]  = datetime.strptime(row[5], TIME_FMT).time()
    row[6]  = float(row[6])
    row[7]  = datetime.strptime(row[7], TIME_FMT).time()
    row[8]  = float(row[8])
    row[9]  = float(row[9])
    row[10] = float(row[10])
    return Flight(*row[:11])

def notHeader(row):
    return "Description" not in row
#airlines.filter(notHeader).take(10)    



if __name__ == "__main__":

  conf = SparkConf().setAppName("average")
  sc = SparkContext(conf=conf)

  #setting log level to error                                                                                                                                                                               
 # val rootLogger = Logger.getRootLogger()                                                                                                                                                                  
 # rootLogger.setLevel(Level.ERROR)                                                                                                                                                                         

  #importing data from HDFS for performing analysis                                                                                                                                                         
  airlines = sc.textFile("hdfs://192.168.0.156:8020/airlines/airlines.csv")
  flights = sc.textFile("hdfs://192.168.0.156:8020/airlines/flights.csv")
  airports =sc.textFile("hdfs://192.168.0.156:8020/airlines/airports.csv")



  # airlinesParsed= airlines.filter(notHeader).map(split)                                                                                                                                                   
  airlinesParsed = dict(airlines.map(split).collect())
  airportsParsed= airports.filter(notHeader).map(split)
 # print "without header and spliting up", airlines.take(10)                                                                                                                                                
  flightsParsed= flights.map(lambda x: x.split(',')).map(parse)
  #print "The average delay is "+str(sumCount[0]/float(sumCount[1]))                                                                                                                                        
  airportDelays = flightsParsed.map(lambda x: (x.origin,x.dep_delay))
  # First find the total delay per airport                                                                                                                                                                  
  airportTotalDelay=airportDelays.reduceByKey(lambda x,y:x+y)

   # Find the count per airport                                                                                                                                                                              
  airportCount=airportDelays.mapValues(lambda x:1).reduceByKey(lambda x,y:x+y)

  # Join to have the sum, count in 1 RDD                                                                                                                                                                    
  airportSumCount=airportTotalDelay.join(airportCount)
  # Compute avg delay per airport                                                                                                                                                                           
  airportAvgDelay=airportSumCount.mapValues(lambda x : x[0]/float(x[1]))
  airportDelay = airportAvgDelay.sortBy(lambda x:-x[1])
  print "", airportDelay.take(10)
  airportLookup=airportsParsed.collectAsMap()
  #airlineLookup=airlinesParsed.collectAsMap()                                                                                                                                                              
  airline_lookup = sc.broadcast(airlinesParsed)
  delays  = flightsParsed.map(lambda f: (airline_lookup.value[f.airline],add(f.dep_delay, f.arv_delay)))
  delays  = delays.reduceByKey(add).collect()
  delays  = sorted(delays, key=itemgetter(1))
  #tenairlines = delays.map(toCSVLine)                                                                                                                                                                      
  ten = airportAvgDelay.map(lambda x: (airportLookup[x[0]],x[1]))
  #print "", ten.take(10)                                                                                                                                                                                   
  for d in delays:
      print "%0.0f minutes delayed\t%s" % (d[1], d[0])
  airportBC=sc.broadcast(airportLookup)
  topTenAirportsWithDelays = airportAvgDelay.map(lambda x: (airportBC.value[x[0]],x[1])).sortBy(lambda x:-x[1])
  lines = topTenAirportsWithDelays.take(10)
  topten = "/home/hadoop/"
  tenairlines = "/home/hadoop/"
  with open('topten', "w") as output:
      writer = csv.writer(output, lineterminator='\n')
      for val in lines:
          writer.writerows([val])
  with open('tenairlines',"w") as output:
      writer = csv.writer(output, lineterminator='\n')
      for val in delays:
          writer.writerows([val])
  #tenairlines.saveAsTextFile('/home/hadoop/ten.csv')                                                                                                                                                       
  timetaken  = time.time()-timestarted
  print "", timetaken

