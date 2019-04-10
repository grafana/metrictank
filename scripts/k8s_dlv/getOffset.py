#!/usr/bin/python

from kazoo.client import KazooClient
import json
import sys
import time


def default():
  print "oldest"
  sys.exit(0)

if len(sys.argv) < 2:
  print "Usage: %s <topic>" % sys.argv[0]
  sys.exit(1)

topic = sys.argv[1]

zk = KazooClient(hosts='zookeeper:2181')
zk.start()
try:
  resp = zk.get("/config/topics/%s" % topic)
except:
  default()
finally:
  zk.stop()

if len(resp) < 1:
  default()

try:
  data = json.loads(resp[0])
except:
  default()

try:
  retention=int(data.get('config', {}).get('retention.ms', 0))
  segmentSize=int(data.get('config', {}).get('segment.ms', 1800000))
except:
  default()

if retention == 0:
  default()

# oldest we can safetly consume from in minutes
oldest = (retention - segmentSize)/(1000 * 60)

print "%dm" % oldest
