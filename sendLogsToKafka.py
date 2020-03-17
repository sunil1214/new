#!/usr/bin/python

import time
import json
import sys
from kafka import KafkaProducer
import argparse

bootstrap_servers = ['192.168.0.124:6667']
parser = argparse.ArgumentParser(description="Log File")
parser.add_argument('--path', '-p', help= 'paste path to Log file',required=True)
args = parser.parse_args()



# producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

producer = KafkaProducer(bootstrap_servers = bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def follow(syslog_file):
    syslog_file.seek(0,2) # Go to the end of the file
    while True:
        line = syslog_file.readline()
        if not line:
            time.sleep(0.1) # Sleep briefly
            continue
        yield line
        
logfile = open(str(args.path))

loglines = follow(logfile)
print(":in")
for line in loglines:
    producer.send('esxi', line)
    print(line)

