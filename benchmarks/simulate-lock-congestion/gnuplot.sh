#!/bin/bash

file=$(mktemp)
for type in writeq-true-query writeq-true-metric-update writeq-true-metric-add writeq-false-metric-add writeq-false-query writeq-false-metric-update; do
cat << EOF > $file
set title '$type'
set terminal png 
set xlabel 'new-series-pct'
set ylabel 'milliseconds'
set output '$type.png'
plot '$type.txt' using 1:2 with lines title 'mean', '$type.txt' using 1:5 with lines title 'p99'
EOF
gnuplot -p $file
done
