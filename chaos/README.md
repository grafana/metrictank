## chaos

these golang tests spin up the docker-chaos stack (6 instances)
and perform various chaotic operations and confirm correct cluster behavior.
currently working on taking 1 instance down with min-available-shards 12 ("all")
later, will do more various scenarios (multiple shards, all replicas vs some replicas, etc)
and with different min-available-shards-settings.


note, you need these containers: gaiadocker/iproute2, gaiaadm/pumba (actually for now checkout https://github.com/gaia-adm/pumba/pull/58 and build that locally)


