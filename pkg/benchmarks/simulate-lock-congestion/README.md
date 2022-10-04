
runs mt-simulate-lock-congestion benchmarks,
with parameters that mimic problematic situations which we've seen in our production enrivonment,
while varying the newseries percentage, and with and without the index write queue enabled.

parses the measurements output of each run and generates gnuplot charts
