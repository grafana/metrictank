# dur

a library to parse:
* time specifications and convert them to durations.
* datetime specifications and convert them to unix timestamps.

Meant to work like [Graphite's from/until parsing](http://graphite.readthedocs.io/en/latest/render_api.html#from-until).
which aims to support all `at(1)` formats also. So do we. see some notes below.

## Durations

Similar to [time.ParseDuration](https://golang.org/pkg/time/#ParseDuration) which has valid time units "ns", "us" (or "µs"), "ms", "s", "m", "h".
which allows signed sequences and optional fractions like "300ms", "-1.5h" or "2h45m".

But we work with larger units (and more forgiving ways to specify them) in this range:
s/sec/secs/second/seconds, m/min/mins/minute/minutes, h/hour/hours, d/day/days, w/week/weeks, mon/month/months, y/year/years
We however do not allow fractions or negative numbers.
Note that this library takes a shortcut to provide convenience: a day = 24 hours, week = 7days, mon = 30 times 24 hours, year is 365 days.
If these inaccuracies are a problem to you, do your own date math.

E.g.: '7d', '1y6mon', '1hour15min3s'


## Datetimes

Functions to convert human specified date time specifications into timestamps.
Supported formats:
* `now-<duration>` now with duration subtracted. (for duration formats see above)
* `-<duration>`: shortcut for the above
* `now+<duration>` now with duration added. (for duration formats see above)
* `HH:MM_YYMMDD`
* `YYYYMMDD`
* `MM/DD/YY` 
* `integer`: unix timestamp
* `<monthname> <num>` like `january 3` or `march 8`
* `monday`, `tuesday` last (or current day if matching) day.
* `at` formats : `now`, `today`, `midnight`, `noon`, `teatime`, `midnight yesterday`, `6pm tomorrow`, `3AM yesterday`

See the code and unit tests for details.

## Notes on compatibility and implementation.

* Graphite [#1691](https://github.com/graphite-project/graphite-web/issues/1691): `monthname+num` goes back in time and clears HH/MM fields (unlike at): we follow graphite.
* Graphite applies the appropriate timezone (given via tz argument or defaulting to what's configured in the config file) to:
  1) the interpretation of datetime specifications where applicable.
  2) the rendering of output timestamps in images (does not apply for metrictank)
  Though [#639](https://github.com/graphite-project/graphite-web/issues/639) shows (1) doesn't always work (not for at-style patterns). Here we apply this consistently.
* Graphite [#263](https://github.com/graphite-project/graphite-web/issues/263) shows that patterns like "6pm+yesterday" sometimes don't work.  Here it does.
* Graphite doesn't seem to support [all the at syntax](https://www.computerhope.com/unix/uat.htm), and we aim to support the same as Graphite for now (which is already quite much!).
  Going 100% is not a high prio for now.

