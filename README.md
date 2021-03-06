# NextBus Util

These are a small collection of scripts
that log data from [NextBus](https://www.nextbus.com/)
using their [API](https://www.nextbus.com/xmlFeedDocs/NextBusXMLFeed.pdf).

* `nb-vstop`: Polls NextBus for the vehicle locations
  of a specified agency/route
  and (this is the novel part)
  for each vehicle, its closest stop.

* `nb-vclosest`: Filters the output of `nb-vstop`
  such that,
  for each vehicle/stop pair,
  selects only the log entries
  where the vehicles' distance to the stops is closest.

## Splunk Integration

Additionally,
there are historic streetcar fleet and stop data lookup tables
for the
[San Francisco Municipal Railway (Muni)](https://www.sfmta.com/)
as well as a custom search command
for integration with [Splunk](https://www.splunk.com/).

## Non-Affiliation

This software is not affiliated with
NextBus, SFMTA, or Splunk.
