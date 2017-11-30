[![Go Report Card](https://goreportcard.com/badge/github.com/Dieterbe/profiletrigger)](https://goreportcard.com/report/github.com/Dieterbe/profiletrigger)
[![GoDoc](https://godoc.org/github.com/Dieterbe/profiletrigger?status.svg)](https://godoc.org/github.com/Dieterbe/profiletrigger)

automatically trigger a profile in your golang (go) application when a condition is matched.

# currently implemented:

* when process obtains certain number of bytes from the system, save a heap (memory) profile
* when cpu usage reaches a certain percentage, save a cpu profile.

# demo

see the included cpudemo and heapdemo programs, which gradually add more and cpu and heap utilisation, to show the profiletrigger kicking in.
