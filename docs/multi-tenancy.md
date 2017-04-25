# Multi-tenancy

A metrictank based stack is multi-tenant. Here's how it works:

* Tenants, or organisations, have their own data stored under their orgId.
* Metrictank isolates data in storage based on the org-id, during ingestion as well as retrieval with the http api.
* During ingestion, the org-id is set in the data coming in through kafka, or for carbon input plugin, is set to 1.
* For retrieval, metrictank requires an x-org-id header.
* Requests sent to Graphite must include a "x-org-id" header.  This header will be passed from graphite through to metrictank
* For a secure setup, you must make sure these headers cannot be specified by users. You may need to run something in front to set the header correctly after authentication
  (e.g. [tsdb-gw](https://github.com/raintank/tsdb-gw)
* orgs can only see the data that lives under their org-id, and also public data
* public data is stored under orgId -1 and is visible to everyone.
