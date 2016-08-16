# Tags

Metrictank uses [metrics2.0](http://metrics20.org/) for structuring metrics and naming them.
Note that tags are modeled as an array of strings, where you can have `key:value` tags but also simply `value` tags.

While metrictank can ingest and store data in metrics2.0 format, making use out of this data is still on ongoing project.

Here are some goals:

* automatically setting the right unit and axis labels in grafana
* automatically converting available data to the requested unit for displaying
* automatically merging series (if you send a series first as a time in ms and then s, we can intelligently merge)
* automatically setting consolidation parameters based on the mtype tag

