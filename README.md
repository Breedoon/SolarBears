# SolarEdge vs Solectria Data Analysis

This repository contains the code for conducting a statistical analysis of the daily differences in performance between subsets of two manufacturers of solar panels, Solectria (n=104) and SolarEdge (n=33), adjusted by system size, time of the year, and location.
Solectria data was acquired from Solectria's [publicly accessible sites](https://solrenview.com/cgi-bin/cgihandler.cgi?&sort=site_name&logo) while SolarEdge data was provided to us by our Civic Partner [@DennyLehman](https://github.com/DennyLehman) from a private database.

We found that, on average, Solectria sites perform about 2% better than the SolarEdge ones, although occasional daily differences can reach as far as -8% to 8% (SolarEdge performance - Solectria performance).

![Dataviz of differences](dataviz/diffs.png?raw=true "Differences plot")

---

Also, the file _fetchers/fetcher_csv.py_ contains an improvised fetcher/transformer for Solectria inverter production data based on a link from Solectia Monitoring Platform's 'Download CSV' button (function _get_historical_data()_), because the official Solectira API does not split the requested data into batches, but simply gives the total production for the specified period so, for example, fetching a year of production in 10-minute intervals for one site would take:
* With Solectria API: 6 (10-minute intervals in an hour) * 24 (hours in a day) * 365 (days in a year) * 6 (seconds per request - Solectria API's hard limit) = 315,360 seconds or __3.65 days__.
* With the CSV fetcher: 53 (weeks in a year - gives 10-minute batches if requesting weekly) * 1 (seconds per request - no hard limit, so depends on the internet connection) ≈ __53 seconds__.

---

Slides from the presentation of this project are available [here](https://docs.google.com/presentation/d/19MPIvQCc7ZCe80es246g0bGVtcQiKZEYB97wPwenq4s). 
