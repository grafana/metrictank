# Usage reporting

Every organisation will have 2 series automatically appear in their metrics tree:

* `metrictank.usage.numSeries`: the number of series seen for the given organisation, measured each `accounting-period` (see config)
* `metrictank.usage.numPoints`: a running counter that tracks how many points have been received for the given organisation.

The master org, org 1, will also have the following:

* `metrictank.usage-minus1.numSeries`
* `metrictank.usage-minus1.numPoints`

These are the equivalents of the above, though they measure the number of series and points under the special org -1.


These series are automatically populated as data for given organisations is seen.  They are stored just like any other series.
