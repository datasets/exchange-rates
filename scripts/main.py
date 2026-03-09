import csv
import io
import tempfile
from pathlib import Path

import requests

from country_codes import country_codes, inverse

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
HEADER = ["Date", "Country", "Exchange rate"]


def values(frequency):
    output_array = []
    errors = []
    for country in country_codes:
        if frequency == "daily":
            type_num = 0
        elif frequency == "monthly":
            type_num = 1
        else:
            type_num = 2
        specific_part = country_codes[country][type_num]
        if specific_part != "":
            url = base_url + specific_part
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
                    output_array.append(country_array)
            except requests.RequestException as exc:
                errors.append(f"{country}: {exc}")

    if errors:
        raise RuntimeError(
            f"Failed to fetch {frequency} exchange rates for {len(errors)} countries: "
            + "; ".join(errors)
        )

    if not output_array:
        raise RuntimeError(f"No rows fetched for {frequency} exchange rates")

    return output_array


def print_to_csv(output, location):
    destination = Path(location)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", newline="", delete=False, dir=destination.parent, encoding="utf-8"
    ) as tmp_file:
        writer = csv.writer(tmp_file)
        writer.writerow(HEADER)
        writer.writerows(output)
        temp_path = Path(tmp_file.name)

    temp_path.replace(destination)


DATA_DIR.mkdir(parents=True, exist_ok=True)
print_to_csv(values("daily"), str(DATA_DIR / "daily.csv"))
print_to_csv(values("monthly"), str(DATA_DIR / "monthly.csv"))
print_to_csv(values("yearly"), str(DATA_DIR / "annual.csv"))
