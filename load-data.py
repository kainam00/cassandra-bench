#!/usr/bin/env python
from cassandra.cluster import Cluster
import os, cassandra, threading, time

# Predefined variables
# Total records to load
totalRecords=1000000
# Batch size
batchsize=1000
# Number of threads to run
threadnum=4


class insertBatch (threading.Thread):
    def __init__(self, startkey, batchsize):
        threading.Thread.__init__(self)
        self.batchsize = batchsize
        cluster = Cluster(['0.0.0.0'])
        self.session = cluster.connect('bench')
        self.startkey = startkey

    def run(self):
        # Generate data
        data = {}
        for i in range(0,self.batchsize):
            entry = {
                "key": startkey + i,
                "value": os.urandom(5000)
            }
            data[i]=entry

        # Insert data
        for key, value in data.iteritems():
            self.session.execute("INSERT INTO bench_raw(key, value, column1) VALUES(%s, %s, %s)",(value["key"], bytearray(value["value"]), bytearray(1)))
        print ".",

# Connect to the local cassandra cluster
cluster = Cluster(['0.0.0.0'])
session = cluster.connect()

# Create keyspace
try:
    session.execute("CREATE KEYSPACE \"bench\" WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};")
except cassandra.AlreadyExists:
    print "Keyspace already exists... moving on"

session.execute("USE bench")

# Create table
try:
    session.execute("CREATE TABLE bench_raw (key bigint, column1 blob, value blob, PRIMARY KEY (key, column1)) WITH COMPACT STORAGE AND bloom_filter_fp_chance=0.100000 AND populate_io_cache_on_flush='false' AND replicate_on_write='true' AND caching='KEYS_ONLY' AND comment='' AND dclocal_read_repair_chance=0.000000 AND gc_grace_seconds=864000 AND read_repair_chance=0.000000 AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'} AND compression={'sstable_compression': 'SnappyCompressor'};")
except cassandra.AlreadyExists:
    print "Table already exists... moving on"

loadedRecords=0
startkey=0
while loadedRecords < totalRecords:
    threads = []
    start=time.time()

    for i in range(0,threadnum):
        # Create new threads
        thread = insertBatch(startkey, batchsize)
        startkey=startkey+batchsize

        # Start new Threads and store them in an array to keep track
        thread.start()
        threads.append(thread)

    # Wait for threads to finish
    for t in threads:
        t.join()

    loadedRecords = loadedRecords+batchsize*threadnum
    end=time.time()
    print startkey
    print "Loaded " + str(loadedRecords) + " at " + str(batchsize*threadnum/(end-start)) + " records/sec"


