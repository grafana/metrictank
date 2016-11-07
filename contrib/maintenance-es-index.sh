# this file contains some bash functions which come in handy when maintaining an ES metadata index
# (which is deprecated, but anyway. see https://github.com/raintank/metrictank/blob/master/docs/metadata.md )
#
# first source this file:
# source maintenance-es-index.sh
# (note: if your shell is not bash the syntax may not be supported)
#
# then you can call the functions like so:
# es_list some.graphite.*.pattern.*.*
# es_search $(es_query some.graphite.*.pattern.*.*)
# es_delete $(es_query some.graphite.*.pattern.*.*)
# for i in $(cat file-with-prefixes); do es_delete "nodes.n0:$i"; done

# see the documentation for es_query for some important caveats

index=metrictank

# show all matching documents, given a query
function es_search () {
    size=${2:-20}
    curl -s "http://localhost:9200/$index/metric_index/_search?q=$1&pretty=true&size=$size"
}

# delete all matching documents, given a query
function es_delete () {
    curl -s -X DELETE "http://localhost:9200/$index/metric_index/_query?q=$1"
}

# list series matching a graphite pattern
function es_list () {
    es_search $(es_query $1) 10000 | grep name | sed -e 's/^.*: "\|",$//g'
}

# construct a query, given a graphite pattern
# e.g. so*.*.and.so -> nodes.n0:so*+AND+nodes.n2:and+AND+nodes.n3:so
# caveats:
# - does not support {foo,bar} or [this,kindof] graphite syntax! just literal words and wildcards.
function es_query () {
    local q
    IFS='.' read -ra nodes <<< "$1"
    for i in "${!nodes[@]}"; do
        if [ "${nodes[$i]}" == '*' ]; then
            new="%2B_exists_:nodes.n$i"
        else
            new="%2Bnodes.n$i:%22${nodes[$i]}%22"
        fi
        [ -n "$q" ] && q="$q+"
        q="$q$new"
    done
    i=$(expr $i + 1)
    q="$q+-_exists_:nodes.n$i"
    echo "$q"
}
