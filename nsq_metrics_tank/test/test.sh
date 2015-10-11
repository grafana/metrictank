#!/bin/bash
wget 'http://localhost:6063/get?render=litmus.fake_org_79_endpoint_2.dev1.http.ok_state' -q -O - | python -mjson.tool | grep -c Val
echo 'GET http://localhost:6063/get?render=litmus.fake_org_79_endpoint_2.dev1.http.ok_state' | vegeta attack -rate 1000 -duration=10s | vegeta report
