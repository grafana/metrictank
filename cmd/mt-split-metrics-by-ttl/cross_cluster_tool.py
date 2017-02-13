#!/usr/bin/env python
#
# This script is intended for a cross-cluster migration
# scenario.
#
# It's intended to be run on the source cluster and it
# assumes that the new style schema has already been
# created on the destination cluster by a new instance
# of metrictank.
# This script does not execute any commands directly,
# it only prints a shell script that can be piped into
# a shell.
#
# It will perform the following steps:
#   - take a snapshot of the data
#   - symlink the directories to match the
#     destination tables
#   - import the data into the destination cluster


import math
import os
import argparse

parser = argparse.ArgumentParser(description='Cross cluster migration tool')
parser.add_argument(
    '--source-keyspace',
    dest='src_kspace',
    type=str,
    help='source keyspace',
    default='metrictank',
    required=False,
)
parser.add_argument(
    '--destination-keyspace',
    dest='dst_kspace',
    type=str,
    help='destination keyspace',
    default='metrictank',
    required=False,
)
parser.add_argument(
    '--source-table',
    dest='src_table',
    type=str,
    help='source table name',
    default='metrics',
    required=False,
)
parser.add_argument(
    '--destination-table',
    dest='dst_table',
    type=str,
    help='first part of destination table name',
    default='metrics_',
    required=False,
)
parser.add_argument(
    '--destination-address',
    dest='dst_addr',
    type=str,
    help='destination cluster address ip:port',
    default='127.0.0.1:9042',
)
parser.add_argument(
    '--cassandra-data-dir',
    dest='data_dir',
    type=str,
    help='cassandra data directory',
    default='/var/lib/cassandra/data',
    required=False,
)
parser.add_argument(
    '--temp-dir',
    dest='tmp_dir',
    type=str,
    help='temporary dir to create symlink structure in',
    default='/tmp',
    required=False,
)
parser.add_argument(
    '--snapshot',
    dest='snapshot',
    type=str,
    help='name of the snapshot',
    default='migration-1',
    required=False,
)
parser.add_argument(
    'ttls',
    nargs='+',
    type=float,
    help='list of TTLs in hours',
)


args = parser.parse_args()
dst_tables = set(
    '{table_name}{bucket}'.format(
        table_name=args.dst_table,
        bucket=int(2**math.floor(math.log(ttl, 2))),
    )
    for ttl in args.ttls
)
all_cmds = []


def chomp_slashes(string):
    while string[-1:] == '/':
        string = string[:-1]
    return string


data_dir, tmp_dir = [chomp_slashes(x) for x in [args.data_dir, args.tmp_dir]]


def append_cmds(cmds):
    for cmd in cmds:
        append_cmd(cmd)


def append_cmd(cmd):
    if len(all_cmds) > 0:
        all_cmds[-1] = all_cmds[-1] + ' &&'
    all_cmds.append(cmd)


def snapshot():
    cmd = (
        'nodetool snapshot -t {snapshot} --table {src_table} {src_keyspace}'
        .format(
            snapshot=args.snapshot,
            src_table=args.src_table,
            src_keyspace=args.src_kspace,
        )
    )
    append_cmd(cmd)


def symlink():

    cmds = ['mkdir -p {tmp_dir}'.format(tmp_dir=tmp_dir)]
    cmds.extend(
        'ln -s '
        '{data_dir}/{src_kspace}/{src_table}/snapshots/{snapshot} '
        '{tmp_dir}/{dst_kspace}/{table}'
        .format(
            data_dir=data_dir,
            src_kspace=args.src_kspace,
            src_table=args.src_table,
            snapshot=args.snapshot,
            tmp_dir=tmp_dir,
            dst_kspace=args.dst_kspace,
            table=table,
        )
        for table in dst_tables
    )
    append_cmds(cmds)


def load():
    cmds = [
        'sstableloader -d {dst_addr} {tmp_dir}/{dst_kspace}/{table}'
        .format(
            dst_addr=args.dst_addr,
            tmp_dir=tmp_dir,
            dst_kspace=args.dst_kspace,
            table=table,
        )
        for table in dst_tables
    ]
    append_cmds(cmds)


steps = [
    snapshot,
    symlink,
    load,
]

for step in steps:
    step()

for cmd in all_cmds:
    print(cmd)
