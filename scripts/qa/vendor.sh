#!/bin/bash

if ! which dep >/dev/null; then
	go get -u github.com/golang/dep/cmd/dep || exit 1
fi

dep version

# until we have https://docs.google.com/document/d/1j_Hka8eFKqWwGJWFSFedtBsNkFaRN3yvL4g8k30PLmg/edit#
# this should do fine:
# (note dep ensure -dry-run and dep ensure would add a whole bunch of packages to vendor, which dep prune deletes again, so we can't just check those)
# we can expect this to change soon though: https://github.com/golang/dep/issues/944

dep ensure -no-vendor -dry-run
ret=$?

dep status

exit $ret
