#!/usr/bin/env python
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import os, cassandra, threading, time, sys

# Predefined variables
batchsize=1000
# Place to write results
outFile = open(os.devnull,"w")
#outFile = open('/tmp/dumpfile',"w")


# Connect to the local cassandra cluster
cluster = Cluster(['0.0.0.0'])
session = cluster.connect()
session.execute("USE bench")
paging = {}
query = "SELECT * FROM bench_raw"
statement = SimpleStatement(query, fetch_size=batchsize)

# Execute the first query and save the paging state
results = session.execute(statement)
paging['paging_state'] = results.paging_state

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
