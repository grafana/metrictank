// Package conf reads config data from two of carbon's config files
// * storage-schemas.conf (old and new retention format)
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
// * storage-aggregation.conf
// see http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-aggregation-conf
//
// it also adds defaults (the same ones as graphite),
// so that even if nothing is matched in the user provided schemas or aggregations,
// a setting is *always* found
// uses some modified snippets from github.com/lomik/go-carbon and github.com/lomik/go-whisper
package conf
