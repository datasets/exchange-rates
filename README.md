Foreign exchange rates from US Federal Reserve in daily, monthly and yearly basis.

## Data

Data is gathered from [https://fred.stlouisfed.org](https://fred.stlouisfed.org).
Most of the countries have rates for days, months and years, but some only have for months.
Some countries have inverted values. Most are compared to USD, and some are USD compared to them.

Following country currencies have `USD/currency` ratio:

* Austalia
* Euro
* Ireland
* New Zealand
* United Kingdom

The rest of countries have `currency/USD` ratio.

In this dataset, there are 3 granularities available:

* daily
* monthly
* yearly

The data has following fields:

* Date - date in ISO format
* Country - name of a country
* Value - currency rate

## Preparation

You will need Python 3.6 or greater and dataflows library to run the script

To update the data run the process script locally:

```
# Install dataflows
pip install dataflows

# Run the script
python exchange_rates_flow.py
```

## License

Licensed under the [Public Domain Dedication and License][pddl] (assuming
either no rights or public domain license in source data).

[pddl]: http://opendatacommons.org/licenses/pddl/1.0/
