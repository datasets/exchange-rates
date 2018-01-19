This is a scraping tool for getting exchange rates updated daily, monthly and annually.

## Data

Data is gathered from[https://fred.stlouisfed.org](https://fred.stlouisfed.org).
Most of the countries have rates for days, months and years, but some only have for months.
For every country there is a link to a .txt file. With explanation in header and table of data.
Some countries have inverted values. Most are compared to USD, and some are USD compared to them.

List of countries where U.S/Foreign Exchange Rate:

Austalia
Ireland
New Zealand 
United Kingdom

Euro is in the form of US/EURO

The rest countries have Foreign Exchange Rate/U.S


As the output three files are generated:

* daily.csv
* monthly.csv
* yearly.csv

CSV files have columns:

* Date
* Country
* Value

## Preparation

Run python script:
```
scripts/main.py
```

## License

Licensed under the [Public Domain Dedication and License][pddl] (assuming
either no rights or public domain license in source data).

[pddl]: http://opendatacommons.org/licenses/pddl/1.0/