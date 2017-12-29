our chaos stack uses pumba and previously we've also used toxiproxy to trigger certain failures.

# requirements
* can kill, pause, network partition, network delays, etc
* has api
* udp support

# nice to have
* stateful

# options

## toxiproxy
+ Go
+ stateful
+ nice api/client lib
- application level, needs app level changes: advertise addr, nslookup, config changes etc
- NO udp

## https://github.com/worstcase/blockade
- no programmatic api?
- python

## https://github.com/giantswarm/mizaru
- no recent updates

## https://github.com/gaia-adm/pumba 
+ Go
+ recent updates/popular. looks solid code.
+ can kill container
- command based, not stateful. no api.

## https://github.com/tomakehurst/saboteur
- assumes control over iptables. probably doesn't work with docker?
