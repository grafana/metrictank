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
  * `planRequests`: this plans all models.Req objects, deciding which archive to read from, whether to apply normalization, etc.
    (see NOTES in expr directory for more info)
  * `getTargets`: gets the data from the local node and peer nodes based on the models.Req objects
  * `mergeSeries`: if there's multiple series with same name/tags, from, to and consolidator (e.g. because there's multiple series because users switched intervals), merge them together into one series
  * Sort each merged series so that the output of a function is well-defined and repeatable.
  * `plan.Run`:  invoke all function processing, followed by runtime consolidation as necessary

## MDP-optimization

MDP at the leaf of the expr tree (fetch request) of 0 means don't optimize.  If set it to >0 it means the request can be optimized.
When the data may be subjected to a GR-function, we set it to 0.
How do we achieve this?
* MDP at the root is set 0 if the request came from graphite or to MaxDataPoints otherwise.
* as the context flows from root through the processing functions to the data requests, if we hit a GR function, we set MDP to 0 on the context (and thus also on any subsequent requests)

## Pre-normalization

Any data requested (checked at the leaf node of the expr tree) should have its own independent interval.
However, multiple series getting fetched that then get aggregated together may be pre-normalized if they are part of the same pre-normalization-group (have a common PNGroup that is > 0).
(for more details see devdocs/alignrequests-too-course-grained.txt)
The mechanics here are:
* we set PNGroup to 0 by default on the context, which gets inherited down the tree
* as we traverse down tree: transparent aggregations set PNGroups to the pointer value of that function, to uniquely identify any further data requests that will be fed into the same transparent aggregation.
* as we traverse down, any opaque aggregation functions and IA-functions reset PNGroup back to 0. Note that currently all known IA functions are also GR functions and vice versa. Meaning,
  as we pass functions like smartSummarize which should undo MDP-optimization, they also undo pre-normalization.


## MaxDataPoints

The flow is described below in detail, but basically:
* we take the incoming MDP value (set it 0 if came from graphite) and put that in the Plan. This value is consulted to know what to MDP optimize against (if applicable), to apply runtime consolidation (if applicable) and to report on the number of points that will be emitted.
* the MDP value from the plan is passed through the Context into the expr.Req types. (but is cleared if the optimization is disabled or by certain functions. See "MDP-optimization" above)
This value is used only to control whether or not to apply MDP-optimization when planning the fetching of the request.

mdp set from GET param, but 0 if came from graphite
    -> NewPlan()
        -> plan.MaxDatapoints 
        -> Context.MDP, though GR functions like (smart)Summarize set r.MDP =0
            -> expr.NewReqFromContext()
                -> expr.Req.MDP 
                    -> executePlan() models.NewReq() -> models.Req.MaxPoints
                        -> planRequests(): used for MDP-optimization
            -> plan.MaxDatapoints used for final runtime consolidation
	    -> and also used in planRequests() for reporting
