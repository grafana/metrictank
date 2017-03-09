#!/usr/bin/env python


# this is a tool to verify if the data imported via the whisper importer is
# actually correct. it uses two inputs, the output of whisper-dump.py and the
# output of mt-store-cat.
#
# pipe the whisper-dump.py output into a file:
# $> whisper-dump.py storage/whisper/some/id/of/a/metric/1.wsp  > /tmp/whisper_dump
#
# pipe the mt-store-cat output into another file:
# $> mt-store-cat -print-ts -from 1487764800 -to 1487785801 normal id 1.d588ebb28e4d2ca64d828cb1eb6066d4 5h > /tmp/mt_store_dump
#
# compare the two:
# $> ./verify.py /tmp/whisper_dump /tmp/mt_store_dump
#
#
# averaged aggregations are a special case. mt-store-cat only dumps raw series
# such as sum/cnt/max, but it cannot return an avg series like metrictank does
# because that data is not kept as a raw series in metrictank. given this
# limitation the closest thing we can get to validating avg series is getting
# mt-store-cat to dump the sum series and then divide it by the count to turn
# it into an average again.
# since mt-store-cat has no way to divide we pass a number to this script and
# divide in this script.
#
# $> ./verify.py /tmp/whisper_dump /tmp/mt_store_dump 600

import sys
import re


if len(sys.argv) < 3:
    print(
        'required arguments: {cmd} whisper-dump-output '
        'mt-store-cat-output [factor]'
        .format(cmd=sys.argv[0])
    )
    exit(1)


if len(sys.argv) == 4:
    factor = float(sys.argv[3])
else:
    factor = float(1)


def get_data(path, regex):
    fd = open(path, 'r')
    results = {}
    for line in fd.readlines():
        match = regex.match(line)
        if not match:
            continue
        if len(match.groups()) != 2:
            continue
        results[int(match.group(1))] = float(match.group(2))

    fd.close()
    return results


whisper_regexp = re.compile('^[0-9]+\: ([0-9]+), ([0-9\.]+)$')
mt_store_regexp = re.compile('^[\-\>]\s+([0-9]+) ([0-9\.]+)$')

whisper_data = get_data(sys.argv[1], whisper_regexp)
mt_store_data = get_data(sys.argv[2], mt_store_regexp)


print(
    'got {whisper} lines from whisper and {mt_store} lines from mt_store'
    .format(whisper=len(whisper_data), mt_store=len(mt_store_data))
)

src = whisper_data
dst = mt_store_data

misses, correct, wrong = 0, 0, 0

for ts, val1 in src.items():
    if ts not in dst:
        misses += 1
        continue

    val2 = dst[ts]
    val1 = val1 * factor

    # round the floats to 3 decimals
    if int(val1*100) == int(val2*100):
        correct += 1
    else:
        print('wrong: {src} != {dst}'.format(src=val1, dst=val2))
        wrong += 1

print('misses: {val}'.format(val=misses))
print('correct: {val}'.format(val=correct))
print('wrong: {val}'.format(val=wrong))
