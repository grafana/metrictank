#!/bin/bash

if ! which govendor >/dev/null; then
	go get github.com/kardianos/govendor || exit 1
fi

ret=0

external=$(govendor list +external)
missing=$(govendor list +missing)
unused=$(govendor list +unused)

[ -n "$external" ] && ret=1 && echo -e "packages missing in vendor that are in gopath:\n$external\n"
[ -n "$missing"  ] && ret=1 && echo -e "packages missing in vendor that are not found:\n$missing\n"
[ -n "$unused"   ] && ret=1 && echo -e "unused vendored packages found:\n$unused\n"

echo govendor list: && govendor list
echo -n "govendor version: " && govendor -version

exit $ret
