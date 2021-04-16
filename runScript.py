#!/usr/bin/python
import sys
import random
import os
from datetime import datetime
from time import mktime,strptime,localtime


def randomTimestamp(start, end):
    format = '%Y-%m-%d %H:%M:%S'
    starttime = mktime(strptime(start, format))
    endtime = mktime(strptime(end, format))
    time = starttime + random.random() * (endtime - starttime)
    return mktime(localtime(time))

print("--GENERATING START--")
file_size = int(sys.argv[1])
x_size = 1280
y_size = 1024
userId_pool = 1000000
startdate = '2010-01-01 00:00:00'
enddate = '2050-01-01 00:00:00'
f = open(sys.argv[2],'w')
for i in range(file_size):
    if random.choice(range(10)) != 1:
        x = random.randint(0,x_size)
        y = random.randint(0,y_size)
        userId = random.randint(0,userId_pool)
        timestamp= int(randomTimestamp(startdate, enddate))
        result = str(x) + ' ' + str(y) + ' ' + str(userId) + ' ' + str(timestamp)
        f.write(result + '\n')
    else:
        f.write('error string\n')
f.close()
print("--GENERATING END--")

print("--MAPREDUCE START--")
os.system("hdfs dfs -rm -r "+sys.argv[4])
os.system("hdfs dfs -rm -r "+sys.argv[5])
os.system("hdfs dfs -put "+sys.argv[4]+" "+sys.argv[4])
os.system("yarn jar "+sys.argv[3]+" "+sys.argv[4]+" "+sys.argv[5])
print("--MAPREDUCE END--")

