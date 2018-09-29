#!/bin/bash
# for use with environments that have kafka offset=auto such as docker-cluster and docker-dev-custom-cfg-kafka
# first fill up kafka then restart the mt's with docker compose to observe backfill speed
fakemetrics backfill --kafka-mdm-addr localhost:9092 --kafka-mdm-v2=true --offset $((2*366*24))h --period 1800s --speedup 360000 --mpo 500 
