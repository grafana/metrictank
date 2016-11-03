# this file contains some bash functions which come in handy when maintaining an ES metadata index
# (which is deprecated, but anyway. see https://github.com/raintank/metrictank/blob/master/docs/metadata.md )
# 
# first source this file:
# source maintenance-es-index.sh
# (note: if your shell is not bash the syntax may not be supported)
# 
# then you can call the functions like so:
# es_search $(es_query some.graphite.*.pattern.*.*)
# es_delete $(es_query some.graphite.*.pattern.*.*)
# for i in $(cat file-with-prefixes); do es_delete "nodes.n0:$i"; done

# see the documentation for es_query for some important caveats

index=metrictank

# show all matching documents, given a query
function es_search () {
	curl "http://localhost:9200/$index/metric_index/_search?q=$1&pretty=true"
}

# delete all matching documents, given a query
function es_delete () {
        curl -X DELETE "http://localhost:9200/$index/metric_index/_query?q=$1"
}

# construct a query, given a graphite pattern
# e.g. so*.*.and.so -> nodes.n0:so*+AND+nodes.n2:and+AND+nodes.n3:so
# caveats:
# - foo.*.* will also match foo or foo.* or foo.*.*.* since we don't have a check yet for number of fields
# - does not support {foo,bar} or [this,kindof] graphite syntax! just literal words and wildcards.
function es_query () {
    local q
    IFS='.' read -ra nodes <<< "$1"
    for i in "${!nodes[@]}"; do
        [ "${nodes[$i]}" == '*' ] && continue
        new="nodes.n$i:${nodes[$i]}"
        [ -n "$q" ] && q="$q+AND+"
        q="$q$new"
    done
    echo "$q"
}
