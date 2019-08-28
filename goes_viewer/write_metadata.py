from functools import partial
import json


from pyproj import transform
import requests


from goes_viewer import config
from goes_viewer.constants import WEB_MERCATOR, GEODETIC


def filter_func(filters, item):
    for k, v in filters.items():
        if k not in item:
            return False
        else:
            if item[k] not in v:
                return False
    return True


def parse_metadata(url, filters, auth=()):
    req = requests.get(url, auth=auth)
    req.raise_for_status()
    out = {}
    js = req.json()["Metadata"]
    filtered = filter(partial(filter_func, filters), js)
    for site in filtered:
        pt = transform(
            GEODETIC, WEB_MERCATOR, site["Longitude"], site["Latitude"], always_xy=True
        )
        out[site["Name"]] = pt

    return [{'name': k, 'x': v[0], 'y': v[1]} for k, v in out.items()]


if __name__ == "__main__":
    out = parse_metadata(
        config.API_URL, config.FILTERS, auth=(config.API_USER, config.API_PASS)
    )
    with open(config.BASE_DIR / "metadata.json", "w") as f:
        json.dump(out, f)
