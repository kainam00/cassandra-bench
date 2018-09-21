#!/usr/bin/env python
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import os, cassandra, threading, time, sys

# Predefined variables
batchsize=1000
startKey=1000
endKey=2000000
# Place to write results
outFile = open(os.devnull,"w")
#outFile = open('/tmp/dumpfile',"w")


# Connect to the local cassandra cluster
cluster = Cluster(['0.0.0.0'])
session = cluster.connect()
session.execute("USE bench")
currentKey=startKey
found=0
queried=0
start=time.time()
while currentKey < endKey:
    query = "SELECT * FROM bench_raw where key=" + str(currentKey)
    statement = SimpleStatement(query)
    results = session.execute(statement)
    currentKey = currentKey+1

    queried=queried+1

    numResults=len(results.current_rows)
    if numResults > 0:
        found=found+1

    # Write to file
    for row in results:
        outFile.write(str(row.key) + "," + str(row.value) +"\n")

    # Output every batchsize number of queries
    if queried % batchsize == 0:
        end=time.time()
        print "Queried " + str(queried) + ". Found " + str(found) + ". At " + str(batchsize/(end-start)) + " queries/second"
        start=time.time()


sys.exit(1)

# Keep iterating through pages
numResults = batchsize
last = False
while paging['paging_state'] != None:
    start=time.time()
    try:
        # save the paging_state somewhere and return current results
        # resume the pagination sometime later...
        statement = SimpleStatement(query, fetch_size=batchsize)
        ps = paging['paging_state']
        results = session.execute(statement, paging_state=ps)
        paging['paging_state'] = results.paging_state

    except:
        break

    numResults = int(numResults) + len(results.current_rows)
    #print "Got " + str(numResults)
    for row in results:
        outFile.write(str(row.key) + "," + str(row.value) +"\n")
        #print str(row.key)
    end=time.time()
    print "Queried " + str(numResults) + " at " + str(batchsize/(end-start)) + " records/sec"
