#!/usr/bin/python
import yaml
import os
import re

stream = open('/etc/graphite-api.yaml', 'r')
config = yaml.load(stream)
stream.close()

def numCpus():
    try:
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$',
                open('/proc/self/status').read())
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                return res
    except IOError:
        pass
    # if we could not find the number of CPUs use 1    
    return 1

def parseEnv(name, value, cnf):
	if name in cnf:
		cnf[name] = value
		return
	parts = name.split('_')
	pos = 0
	found = False
	while not found and pos < len(parts):
		pos = pos + 1
		group = '_'.join(parts[:pos])
		if group in cnf:
			found = True
			parseEnv('_'.join(parts[pos:]), value, cnf[group])

	if not found:
		print "%s not found in config" % name

# default concurrency to number of cpus * 2
concurrency = numCpus() * 2

for name,value in os.environ.items():
	if name.startswith('GRAPHITE_'):
		name = name[9:]
		parseEnv(name, value, config)
        if name == 'CONCURRENCY':
            concurrency = int(value)

stream = open('/etc/graphite-api.yaml', 'w')
yaml.dump(config, stream, default_flow_style=False)
stream.close()

args = [
  "graphite-api",
  "-b", "0.0.0.0:8888",
  "-w", '%d' % concurrency,
  "--log-level", "debug",
  "graphite_api.app:app",
]
os.execv('/usr/local/bin/gunicorn', args)
#from graphite_api.app import app

#app.run(debug=True, port=8888, host="0.0.0.0", use_reloader=False)

