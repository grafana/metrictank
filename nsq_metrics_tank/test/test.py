#!/usr/bin/env python2

import requests
from pprint import pprint
import time

now = int(time.time())
start = now - (now % 120)
oldest_start = start - 5*120

print ("now: %d - start %d"%( now, start))
print ("oldest start should be %d" % oldest_start)
url = 'http://localhost:6063/get?render=litmus.localhost.dev1.dns.ok_state'
r = requests.get(url)
gaps = {}
data = r.json()
prevTs = None
for t in data:
    firstTs = t['Datapoints'][0]['Ts']
    lastTs = t['Datapoints'][len(t['Datapoints']) -1]['Ts']
    print(t['Target'])
    for dp in t['Datapoints']:
        if prevTs is not None:
            gap = dp['Ts'] - prevTs
            if gap in gaps:
                gaps[gap] += 1
            else:
                gaps[gap] = 1
        #print(dp['Ts'], dp['Ts'] - oldest_start)
        prevTs = dp['Ts']
        lastTs = dp['Ts']
    print("gaps:")
    pprint(gaps)
    print ("last ts age", now - lastTs)
    print ("first ts age", now - firstTs, "expected", now - oldest_start, "diff", oldest_start - firstTs)
