from dataflows import Flow, PackageWrapper, ResourceWrapper, validate
from dataflows import add_metadata, dump_to_path, set_type
import urllib.request


base_url = 'https://fred.stlouisfed.org/data/'
inverse = ['Australia', 'Euro', 'Ireland', 'New Zealand', 'United Kingdom']
country_codes = {
    'Australia': {'daily': 'DEXUSAL', 'monthly': 'EXUSAL', 'yearly': 'AEXUSAL'},
    'Austria': {'daily': '', 'monthly': 'EXAUUS', 'yearly': ''},
    'Belgium': {'daily': '', 'monthly': 'EXBEUS', 'yearly': ''},
    'Brazil': {'daily': 'DEXBZUS', 'monthly': 'EXBZUS', 'yearly': 'AEXBZUS'},
    'Canada': {'daily': 'DEXCAUS', 'monthly': 'EXCAUS', 'yearly': 'AEXCAUS'},
    'China': {'daily': 'DEXCHUS', 'monthly': 'EXCHUS', 'yearly': 'AEXCHUS'},
    'Denmark': {'daily': 'DEXDNUS', 'monthly': 'EXDNUS', 'yearly': 'AEXDNUS'},
    'Euro': {'daily': 'DEXUSEU', 'monthly': 'EXUSEU', 'yearly': 'AEXUSEU'},
    'Finland': {'daily': '', 'monthly': 'EXFNUS', 'yearly': ''},
    'France': {'daily': '', 'monthly': 'EXFRUS', 'yearly': ''},
    'Germany': {'daily': '', 'monthly': 'EXGEUS', 'yearly': ''},
    'Greece': {'daily': '', 'monthly': 'EXGRUS', 'yearly': ''},
    'Hong Kong': {'daily': 'DEXHKUS', 'monthly': 'EXHKUS', 'yearly': 'AEXHKUS'},
    'India': {'daily': 'DEXINUS', 'monthly': 'EXINUS', 'yearly': 'AEXINUS'},
    'Ireland': {'daily': '', 'monthly': 'EXUSIR', 'yearly': ''},
    'Italy': {'daily': '', 'monthly': 'EXITUS', 'yearly': ''},
    'Japan': {'daily': 'DEXJPUS', 'monthly': 'EXJPUS', 'yearly': 'AEXJPUS'},
    'Malaysia': {'daily': 'DEXMAUS', 'monthly': 'EXMAUS', 'yearly': 'AEXMAUS'},
    'Mexico': {'daily': 'DEXMXUS', 'monthly': 'EXMXUS', 'yearly': 'AEXMXUS'},
    'Netherlands': {'daily': '', 'monthly': 'EXNEUS', 'yearly': ''},
    'New Zealand': {'daily': 'DEXUSNZ', 'monthly': 'EXUSNZ', 'yearly': 'AEXUSNZ'},
    'Norway': {'daily': 'DEXNOUS', 'monthly': 'EXNOUS', 'yearly': 'AEXNOUS'},
    'Portugal': {'daily': '', 'monthly': 'EXPOUS', 'yearly': ''},
    'Singapore': {'daily': 'DEXSIUS', 'monthly': 'EXSIUS', 'yearly': 'AEXSIUS'},
    'South Africa': {'daily': 'DEXSFUS', 'monthly': 'EXSFUS', 'yearly': 'AEXSFUS'},
    'South Korea': {'daily': 'DEXKOUS', 'monthly': 'EXKOUS', 'yearly': 'AEXKOUS'},
    'Spain': {'daily': '', 'monthly': 'EXSPUS', 'yearly': ''},
    'Sri Lanka': {'daily': '', 'monthly': 'EXSLUS', 'yearly': ''},
    'Sweden': {'daily': 'DEXSDUS', 'monthly': 'EXSDUS', 'yearly': 'AEXSDUS'},
    'Switzerland': {'daily': 'DEXSZUS', 'monthly': 'EXSZUS', 'yearly': 'AEXSZUS'},
    'Taiwan': {'daily': 'DEXTAUS', 'monthly': 'EXTAUS', 'yearly': 'AEXTAUS'},
    'Thailand': {'daily': 'DEXTHUS', 'monthly': 'EXTHUS', 'yearly': 'AEXTHUS'},
    'United Kingdom': {'daily': 'DEXUSUK', 'monthly': 'EXUSUK', 'yearly': ''},
    'Venezuela': {'daily': 'DEXVZUS', 'monthly': 'EXVZUS', 'yearly': 'AEXVZUS'}
}


def rename(package: PackageWrapper):
    package.pkg.descriptor['resources'][0]['name'] = 'exchange-rates-daily'
    package.pkg.descriptor['resources'][0]['path'] = 'data/daily.csv'
    package.pkg.descriptor['resources'][1]['name'] = 'exchange-rates-monthly'
    package.pkg.descriptor['resources'][1]['path'] = 'data/monthly.csv'
    package.pkg.descriptor['resources'][2]['name'] = 'exchange-rates-yearly'
    package.pkg.descriptor['resources'][2]['path'] = 'data/yearly.csv'
    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    second: ResourceWrapper = next(res_iter)
    third: ResourceWrapper = next(res_iter)
    yield first.it
    yield second.it
    yield third.it
    yield from package


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
                            value = str(round(1 / float(value), 4))

                    yield {
                        'Date': date,
                        'Country': country,
                        'Value': value
                    }
                if 'DATE' in line_str and 'VALUE' in line_str and header:
                    header = False


exchange_rate_flow = Flow(
    add_metadata(
        name="exchange-rates",
        title="Exchange Rates",
        homepage='https://fred.stlouisfed.org',
        licenses="PDDL-1.0",
        version="0.2.0"
    ),
    extract_exchange_rates('daily'),
    extract_exchange_rates('monthly'),
    extract_exchange_rates('yearly'),
    set_type('Date', type='date', format='any'),
    set_type('Value', type='number', format='any'),
    rename,
    validate(),
    dump_to_path(),
)
exchange_rate_flow.process()
