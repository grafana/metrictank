#!/bin/bash

# Performs basic health checks of unit testing of expression functions
# in particular, that each function file has a corresponding test file,
# and that each file contains a test helper that seems to test for some specific failure modes

# the script does not check that the helper method is properly used and implemented, 
# that testing is accurate, etc. 
# that's up to a human to assert!

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..


# list filenames of test files for all non-trivial graphite function implementations
# TODO remove exceptions for smartsummarize once this function "goes live"
# can't just remove the empty stub implementations because smartSummarize is used in TestArgs()
function getExpectedTestFiles () {
    ls -1 expr/func_*.go \
        | grep -v func_get.go \
        | grep -v func_smartsummarize.go \
        | grep -v '_test.go' \
        | sed 's/\.go$/_test.go/' \
        | LC_ALL=en_US sort
}

# list test files for all graphite function implementations
function getActualTestFiles () {
    ls -1 expr/func_*test.go \
        | grep -v func_mock_test.go \
        | LC_ALL=en_US sort
}

ret=0

diff=$(diff <(getExpectedTestFiles) <(getActualTestFiles))
if [ -n "$diff" ]; then
	echo "Found mismatch between graphite functions and their test files!"
	ret=2
	echo "$diff"
fi

for f in $(getActualTestFiles); do
    testFunc=$(sed -n '/func test/,/^}$/p' "$f")
    if [ -z "$testFunc" ]; then
        echo "$f is missing a test helper function!"
        ret=2
    elif [ $(egrep -c 't\.Run\("(DidNotModifyInput|DoesNotDoubleReturnPoints)"' <<< "$testFunc") -ne 3 ]; then
        echo "$f has a test helper function which is missing one of DidNotModifyInput|DoesNotDoubleReturnPoints clauses"
        ret=2
    fi
done

exit $ret
