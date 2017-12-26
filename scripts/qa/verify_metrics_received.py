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
            'perSecond(metrictank.stats.docker-env.default.input.carbon.metrics_received.counter32)',
    },
}

# wait while metrics are being generated
time.sleep(int(sys.argv[3]))

result = requests.post(**request_parameters)

if result.status_code != 200:
    error(
        'received bad response status code: {code} - reason: {reason}'
        .format(code=result.status_code, reason=result.text)
    )

try:
    parsed_result = json.loads(result.text)
except Exception:
    error(
        'failed to parse response: {text}'
        .format(text=result.text)
    )

# verify the format and content of the response is as we expect it
# note : since we got a perSecond(), the first value is always null, we only use points 2 and onwards
if (
        len(parsed_result) < 1 or
        'datapoints' not in parsed_result[0] or
        not all([len(x) >= 2 for x in parsed_result[0]['datapoints']]) or
        not all([
            isinstance(x[0], (int, float))
            for x in parsed_result[0]['datapoints'][1:]
        ])
):
    error(
        'received unexpected response:\n{response}'
        .format(response=result.text)
    )

datapoints = [float(x[0]) for x in parsed_result[0]['datapoints'][1:]]
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
