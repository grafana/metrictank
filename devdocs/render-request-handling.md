## Render (data) requests are handled like so

`/render` maps to `Server.renderMetrics` which has these main sections/steps:

* `expr.parseMany` validates the target strings (queries) and parses them grammatically.

* `expr.NewPlan` sets up the needed processing functions, their arguments (validates input to match the signature) and lets function adjust the context as it flows between the processing steps
   note: context holds from/to timestamp (may change across processing pipeline, e.g. to handle movingAverage queries) and consolidateBy setting, see NOTES in expr directory.
   this. future version: allow functions to mark safe to pre-aggregate using consolidateBy or not
* handle cases like when we need graphite proxying (if we don't support needed functions, etc)
* `executePlan`:
  * finds all series by fanning out the query patterns to all other shards. 
    this gives basically idx.Node back. has the path, leaf, metricdefinition, schema/aggregation(rollup) settings, for each series, as well as on which node it can be found.
  * construct models.Req objects for each serie. this uses the MKey to identify series, also sets from/to, maxdatapoints, etc.
  * `alignRequests`: this looks at all models.Req objects and aligns them to a common step.
    it selects the archive to use, consolidator settings etc (see NOTES in expr directory for more info)
  * `getTargets`: gets the data from the local node and peer nodes based on the models.Req objects
  * `mergeSeries`: if there's multiple series with same name/tags, from, to and consolidator (e.g. because there's multiple series because users switched intervals), merge them together into one series
  * Sort each merged series so that the output of a function is well-defined and repeatable.
  * `plan.Run`:  invoke all function processing, followed by runtime consolidation as necessary
