import urllib.request
from collections import OrderedDict
from pathlib import Path

import numpy as np
import os

import sys
from datapackage import Package

from scripts.country_codes import country_codes, inverse

base_url = 'https://fred.stlouisfed.org/data/'


def values(frequency):
    output_dict = {}
    number = -1
    for country in country_codes:
        if frequency == "daily":
            type_num = 0
        elif frequency == "monthly":
            type_num = 1
        else:
            type_num = 2
        specific_part = country_codes[country][type_num]
        url = base_url+specific_part+'.txt'
        print(url)
        number = number + 1
        if specific_part != '':
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
                    if date in output_dict.keys():
                        output_dict[date][number] = value
                    else:
                        output_dict[date] = []
                        for num in range(len(country_codes.keys())):
                            output_dict[date].append('')
                        output_dict[date][number] = value

                if 'DATE' in line_str and 'VALUE' in line_str and header:
                    header = False

        print('Done')

    ordered = OrderedDict(sorted(output_dict.items(), key=lambda t: t[0]))

    return ordered


def print_to_csv(dict, location):
    file = open(location, 'w')
    hdr = 'Data'
    for cnt in country_codes.keys():
        hdr = hdr + ',' + cnt

    file.write(hdr + '\n')
    for key, vs in dict.items():
        out = key
        for v in vs:
            out = out + ',' + v.strip()
        file.write(out + '\n')

    file.close()


print_to_csv(values('daily'), '../data/daily.csv')
print_to_csv(values('monthly'), '../data/monthly.csv')
print_to_csv(values('yearly'), '../data/yearly.csv')
