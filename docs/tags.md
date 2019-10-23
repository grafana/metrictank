# Tags

Metrictank implements tag ingestion, storage, and querying to be compatible with the [graphite tags feature](https://graphite.readthedocs.io/en/latest/tags.html).

# Meta Tags

Metrictank has a feature called "Meta Tags" which allows a user to dynamically assign virtual tags to metrics based on given criteria. 
To use it the following two feature flags need to be turned on: `memory-idx.meta-tag-support`, `memory-idx.tag-support`

The necessary API calls to setup meta records are documented [here](https://github.com/grafana/metrictank/blob/master/docs/http-api.md#get-meta-records).

## Eventual consistency in cluster deployments

When using Meta Tags in a clustered setup the replication of Meta Tag Rules (Meta Records) happens via the backend store (currently only implemented for Cassandra). When rules are modified on a Metrictank that has index updating of the backend store enabled the modifications get persisted in the store. At a given interval (default 10s) all other Metrictanks which are using this index and which have the Meta Tags feature enabled poll the store for changes, and if they detect a change they load it. 
This means that for changes to get replicated across a cluster, they need to be made on a Metrictank which has index updates enabled (for cassandra `cassandra-idx.update-cassandra-index=true`). 

# Future Metrics2.0 plans

[metrics2.0](http://metrics20.org/)
Note that tags are modeled as an array of strings, where you can have `key:value` tags but also simply `value` tags.

We want to go further with our tag support and adopt metrics2.0 fully, which brings naming conventions and leveraging semantics.

Here are some goals:

* automatically setting the right unit and axis labels in grafana
* automatically converting available data to the requested unit for displaying
* automatically merging series (if you send a series first as a time in ms and then s, we can intelligently merge)
* automatically setting consolidation parameters based on the mtype tag