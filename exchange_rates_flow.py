import os

from dataflows import Flow, validate, add_metadata, set_type, update_resource
import urllib.request


def readme(fpath='README.md'):
    if os.path.exists(fpath):
        return open(fpath).read()


base_url = 'https://fred.stlouisfed.org/data/'
inverse = ['Australia', 'Euro', 'Ireland', 'New Zealand', 'United Kingdom']
country_codes = {
    'Australia': {'daily': 'DEXUSAL', 'monthly': 'EXUSAL', 'annual': 'AEXUSAL'},
    'Austria': {'daily': '', 'monthly': 'EXAUUS', 'annual': ''},
    'Belgium': {'daily': '', 'monthly': 'EXBEUS', 'annual': ''},
    'Brazil': {'daily': 'DEXBZUS', 'monthly': 'EXBZUS', 'annual': 'AEXBZUS'},
    'Canada': {'daily': 'DEXCAUS', 'monthly': 'EXCAUS', 'annual': 'AEXCAUS'},
    'China': {'daily': 'DEXCHUS', 'monthly': 'EXCHUS', 'annual': 'AEXCHUS'},
    'Denmark': {'daily': 'DEXDNUS', 'monthly': 'EXDNUS', 'annual': 'AEXDNUS'},
    'Euro': {'daily': 'DEXUSEU', 'monthly': 'EXUSEU', 'annual': 'AEXUSEU'},
    'Finland': {'daily': '', 'monthly': 'EXFNUS', 'annual': ''},
    'France': {'daily': '', 'monthly': 'EXFRUS', 'annual': ''},
    'Germany': {'daily': '', 'monthly': 'EXGEUS', 'annual': ''},
    'Greece': {'daily': '', 'monthly': 'EXGRUS', 'annual': ''},
    'Hong Kong': {'daily': 'DEXHKUS', 'monthly': 'EXHKUS', 'annual': 'AEXHKUS'},
    'India': {'daily': 'DEXINUS', 'monthly': 'EXINUS', 'annual': 'AEXINUS'},
    'Ireland': {'daily': '', 'monthly': 'EXUSIR', 'annual': ''},
    'Italy': {'daily': '', 'monthly': 'EXITUS', 'annual': ''},
    'Japan': {'daily': 'DEXJPUS', 'monthly': 'EXJPUS', 'annual': 'AEXJPUS'},
    'Malaysia': {'daily': 'DEXMAUS', 'monthly': 'EXMAUS', 'annual': 'AEXMAUS'},
    'Mexico': {'daily': 'DEXMXUS', 'monthly': 'EXMXUS', 'annual': 'AEXMXUS'},
    'Netherlands': {'daily': '', 'monthly': 'EXNEUS', 'annual': ''},
    'New Zealand': {'daily': 'DEXUSNZ', 'monthly': 'EXUSNZ', 'annual': 'AEXUSNZ'},
    'Norway': {'daily': 'DEXNOUS', 'monthly': 'EXNOUS', 'annual': 'AEXNOUS'},
    'Portugal': {'daily': '', 'monthly': 'EXPOUS', 'annual': ''},
    'Singapore': {'daily': 'DEXSIUS', 'monthly': 'EXSIUS', 'annual': 'AEXSIUS'},
    'South Africa': {'daily': 'DEXSFUS', 'monthly': 'EXSFUS', 'annual': 'AEXSFUS'},
    'South Korea': {'daily': 'DEXKOUS', 'monthly': 'EXKOUS', 'annual': 'AEXKOUS'},
    'Spain': {'daily': '', 'monthly': 'EXSPUS', 'annual': ''},
    'Sri Lanka': {'daily': '', 'monthly': 'EXSLUS', 'annual': ''},
    'Sweden': {'daily': 'DEXSDUS', 'monthly': 'EXSDUS', 'annual': 'AEXSDUS'},
    'Switzerland': {'daily': 'DEXSZUS', 'monthly': 'EXSZUS', 'annual': 'AEXSZUS'},
    'Taiwan': {'daily': 'DEXTAUS', 'monthly': 'EXTAUS', 'annual': 'AEXTAUS'},
    'Thailand': {'daily': 'DEXTHUS', 'monthly': 'EXTHUS', 'annual': 'AEXTHUS'},
    'United Kingdom': {'daily': 'DEXUSUK', 'monthly': 'EXUSUK', 'annual': ''},
    'Venezuela': {'daily': 'DEXVZUS', 'monthly': 'EXVZUS', 'annual': 'AEXVZUS'}
}


def extract_exchange_rates(frequency):
    for country in country_codes:
        country_frequency_ticker = country_codes[country][frequency]
        url = base_url + country_frequency_ticker + '.txt'
        if country_frequency_ticker:
            response = urllib.request.urlopen(url)
            lines = response.readlines()
            header = True
            for line in lines:
                line_str = line.decode('utf-8')
                if not header:
                    date_value = line_str.split(' ')
                    date = date_value[0]
                    value = date_value[1:]
                    value = "".join(value).strip()
                    if value == '.':
                        value = ''
                    if country in inverse:
                        if value != '':
                            value = str(1 / float(value))

                    yield {
                        'Date': date,
                        'Country': country,
                        'Exchange rate': value
                    }
                if 'DATE' in line_str and 'VALUE' in line_str and header:
                    header = False


exchange_rate_flow = Flow(
    add_metadata(
        name="us-euro-foreign-exchange-rate",
        title="USA / EUR Foreign Exchange Rate since 1999",
        homepage='https://fred.stlouisfed.org',
        sources=[
            {
                "name": "federal-reserve-bank-st-louis",
                "title": "Federal Reserve Bank of St. Louis",
                "path": "https://fred.stlouisfed.org/categories/158"
            }
        ],
        licenses=[
            {
                "id": "odc-pddl",
                "name": "public_domain_dedication_and_license",
                "version": "1.0",
                "url": "http://opendatacommons.org/licenses/pddl/1.0/"
            }
        ],
        views=[
            {
                "name": "us-to-uk-foreign-exchange-rate",
                "title": "USA / GBP Foreign Exchange Rate since 1971",
                "resources": [
                    {
                        "name": "daily",
                        "transform": [
                            {
                                "type": "filter",
                                "expression": "data['Country'] === 'United Kingdom'"
                            }
                        ]
                    }
                ],
                "specType": "simple",
                "spec": {
                    "type": "line",
                    "group": "Date",
                    "series": [
                        "Exchange Rate"
                    ]
                }
            },
            {
                "name": "us-euro-foreign-exchange-rate",
                "title": "USA / EUR Foreign Exchange Rate since 1999",
                "resources": [
                    {
                        "name": "daily",
                        "transform": [
                            {
                                "type": "filter",
                                "expression": "data['Country'] === 'Euro'"
                            }
                        ]
                    }
                ],
                "specType": "simple",
                "spec": {
                    "type": "line",
                    "group": "Date",
                    "series": [
                        "Exchange Rate"
                    ]
                }
            }
        ],
        version="0.2.0",
        readme=readme()
    ),
    extract_exchange_rates('daily'),
    extract_exchange_rates('monthly'),
    extract_exchange_rates('annual'),
    update_resource('res_1', **{'name': 'daily', 'path':'data/daily.csv', 'dpp:streaming': True}),
    update_resource('res_2', **{'name': 'monthly', 'path':'data/monthly.csv', 'dpp:streaming': True}),
    update_resource('res_3', **{'name': 'annual', 'path':'data/annual.csv', 'dpp:streaming': True}),
    set_type('Date', resources='daily', type='date', description="Date in ISO format"),
    set_type('Country', resources='daily', type='string', description="Name of a country"),
    set_type('Exchange rate', resources='daily', type='number', description="Foreign Exchange Rate to USD. Only AUD, IEP, NZD, GBP and EUR to USD."),
    set_type('Date', resources='monthly', type='date', description="Date in ISO format"),
    set_type('Country', resources='monthly', type='string', description="Name of a country"),
    set_type('Exchange rate', resources='monthly', type='number', description="Foreign Exchange Rate to USD. Only AUD, IEP, NZD, GBP and EUR to USD."),
    set_type('Date', resources='annual', type='date', description="Date in ISO format"),
    set_type('Country', resources='annual', type='string', description="Name of a country"),
    set_type('Exchange rate', resources='annual', type='number', description="Foreign Exchange Rate to USD. Only AUD, IEP, NZD, GBP and EUR to USD."),
    validate()
)


def flow(parameters, datapackage, resources, stats):
    return exchange_rate_flow


if __name__ == '__main__':
    exchange_rate_flow.process()
