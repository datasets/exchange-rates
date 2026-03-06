import csv
import io
from pathlib import Path

import requests

from country_codes import country_codes, inverse

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="


def values(frequency):
    output_array = []
    number = -1
    for country in country_codes:
        if frequency == "daily":
            type_num = 0
        elif frequency == "monthly":
            type_num = 1
        else:
            type_num = 2
        specific_part = country_codes[country][type_num]
        url = base_url + specific_part
        print(url)
        number = number + 1
        if specific_part != "":
            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                reader = csv.DictReader(io.StringIO(response.text))
                for row in reader:
                    date = (
                        row.get("DATE") or row.get("observation_date") or ""
                    ).strip()
                    value = (row.get(specific_part) or "").strip()
                    if not date:
                        continue
                    if value == ".":
                        value = ""
                    if country in inverse:
                        if value != "":
                            value = str(round(1 / float(value), 4))

                    country_array = [date, country, value]
                    print(country_array)
                    output_array.append(country_array)
            except Exception:
                continue

    return output_array


def print_to_csv(output, location):
    file = open(location, "w")
    hdr = "Date,Country,Value"
    file.write(hdr + "\n")
    for triplet in output:
        file.write(triplet[0] + "," + triplet[1] + "," + triplet[2] + "\n")

    file.close()


DATA_DIR.mkdir(parents=True, exist_ok=True)
print_to_csv(values("daily"), str(DATA_DIR / "daily.csv"))
print_to_csv(values("monthly"), str(DATA_DIR / "monthly.csv"))
print_to_csv(values("yearly"), str(DATA_DIR / "yearly.csv"))
