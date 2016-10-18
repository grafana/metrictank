#!/usr/bin/python

import sys
import time
import requests
import json


def error(msg):
    sys.stderr.write(msg + '\n')
    sys.exit(1)


if len(sys.argv) < 4 or not all([x.isdigit() for x in sys.argv[2:]]):
    error(
        '{cmd} host port check_seconds expected_value'
        .format(cmd=sys.argv[0])
    )

request_parameters = {
    'url': 'http://{host}:{port}/render'.format(
        host=sys.argv[1],
        port=sys.argv[2],
    ),
    'params': {
        'from': '-{secs}sec'.format(secs=sys.argv[3]),
        'format': 'json',
        'maxDataPoints': sys.argv[3],
    },
    'data': {
        'target':
            'stats.docker-env.metrictank.metrictank-1.carbon.metrics_received',
    },
}

# wait while metrics are being generated
time.sleep(int(sys.argv[3]))

result = requests.post(**request_parameters)

if result.status_code != 200:
    error(
        'received bad response status code: {code}'
        .format(code=result.status_code)
    )

try:
    parsed_result = json.loads(result.text)
except Exception:
    error(
        'failed to parse response: {text}'
        .format(text=result.text)
    )

# verify the format and content of the response is as we expect it
if (
        len(parsed_result) < 1 or
        'datapoints' not in parsed_result[0] or
        not all([len(x) >= 2 for x in parsed_result[0]['datapoints']]) or
        not all([
            isinstance(x[0], (int, float))
            for x in parsed_result[0]['datapoints']
        ])
):
    error(
        'received unexpected response:\n{response}'
        .format(response=result.text)
    )

datapoints = [float(x[0]) for x in parsed_result[0]['datapoints']]
datapoints_avg = sum(datapoints)/len(datapoints)
expected = float(sys.argv[4])

if datapoints_avg > expected:
    print('All good:')
    print(result.text)
    sys.exit(0)
else:
    error(
        '{received} is lower than {expected}.\n'
        'Based on received data:\n'
        '{data}'
        .format(
            received=datapoints_avg,
            expected=expected,
            data=result.text,
        )
    )
