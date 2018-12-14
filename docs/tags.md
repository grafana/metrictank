# Tags

Metrictank implements tag ingestion, storage, and querying to be compatible with the [graphite tags feature](https://graphite.readthedocs.io/en/latest/tags.html).

# Future Metrics2.0 plans

[metrics2.0](http://metrics20.org/)
Note that tags are modeled as an array of strings, where you can have `key:value` tags but also simply `value` tags.

We want to go further with our tag support and adopt metrics2.0 fully, which brings naming conventions and leveraging semantics.

Here are some goals:

* automatically setting the right unit and axis labels in grafana
* automatically converting available data to the requested unit for displaying
* automatically merging series (if you send a series first as a time in ms and then s, we can intelligently merge)
* automatically setting consolidation parameters based on the mtype tag

