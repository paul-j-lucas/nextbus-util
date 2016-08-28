The `NextBus_stops.csv` file currently contains stop data only for the
[E-Embarcadero](https://www.sfmta.com/getting-around/transit/routes-stops/e-embarcadero)
and
[F-Market & Wharves](https://www.sfmta.com/getting-around/transit/routes-stops/f-market-wharves)
historic streetcar lines
of the
[San Francisco Municipal Transportation Agency](https://www.sfmta.com/)
(Muni).

You may add or replace these data
with any agency's line(s) you wish.
The data can easily be obtained
by using the `nb-vstop` script with the `-c`, `-r`,  and `-S` options, e.g.:

    nb-vstop -r22 -cS

would dump the stop data for the 22-Fillmore Muni line.
