# Solar Bears Data Analysis – SolarEdge vs Solectria  
This repository contains code for conducting a statistical data analysis of the daily differences in performance between a subset of Solectria (n=104) and SolarEdge (n=33) systems, adjusted by system size, time, and location.
Solectria data was acquired from Solectria's [publicly accessible sites](https://solrenview.com/cgi-bin/cgihandler.cgi?&sort=site_name&logo) while SolarEdge data was provided to us by our Civic Partner [@dennyprodigal](https://github.com/dennyprodigal) from a private database.

We found that, on average, Solectria sites perform about 2% better than SolarEdge sites, although occasional daily differences can reach as far as -8% to 8%.

![Dataviz of differences](dataviz/diffs.png?raw=true "Differences plot")

The file fetchers/fetcher_csv.py contains code for fetching Solectria inverter production data based on the link from Solectia Monitoring Platform's 'Download CSV' button (because official Solectira API does not split the requested data into batches, but simply gives total production for the specified period), and loading that data into the database (functions get_historical_data() and collect_data() respectively).    