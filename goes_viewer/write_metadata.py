import datetime as dt
from functools import partial
import json
import logging
from pathlib import Path
import tempfile


from pyproj import transform
import pytz
import requests


from goes_viewer.constants import WEB_MERCATOR, GEODETIC


logger = logging.getLogger(__name__)


def filter_func(filters, item):
    for k, v in filters.items():
        if k not in item:
            return False
        else:
            if item[k] not in v:
                return False
    return True


def parse_metadata(url, filters, auth=(), params={}, timeout=None):
    req = requests.get(f'{url}/metadata', auth=auth, params=params,
                       timeout=timeout)
    req.raise_for_status()
    out = []
    js = req.json()["Metadata"]
    filtered = filter(partial(filter_func, filters), js)
    for site in filtered:
        pt = transform(GEODETIC,
                       WEB_MERCATOR,
                       site["Longitude"],
                       site["Latitude"],
                       always_xy=True)
        out.append({
            "name": site["Name"],
            "x": pt[0],
            "y": pt[1],
            "units": site["Units"],
            "capacity": f"{site['Peak Power']:0.2f}"
        })
    return out


def get_latest_data(url, metadata_list, auth=(), params={},
                    timeout=None):
    logger.info('Getting data for %s sites', len(metadata_list))
    params = params.copy()
    now = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
    params['startat'] = now.strftime('%Y%m%dT%H%M%SZ')
    params['endat'] = now.strftime('%Y%m%dT%H%M%SZ')
    params['beforestart'] = True
    out = []
    for out_dict in metadata_list:
        sp = params.copy()
        sp['id'] = out_dict['name']
        req = requests.get(f'{url}/data', auth=auth, params=sp,
                           timeout=timeout)
        req.raise_for_status()
        otime = 'N/A'
        oval = 'N/A'
        try:
            data = req.json()['Data'][0]
        except IndexError:
            logger.info('No data for %s', out_dict['name'])
        else:
            dtime = dt.datetime.strptime(
                data['BeginAt'],
                '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.utc).astimezone(
                    pytz.timezone('MST'))
            # only if the value is from the past hour, or from the past day and also
            # zero do we include
            if dtime > now - dt.timedelta(hours=1) or (
                    dtime > now - dt.timedelta(days=1)
                    and float(data['Value']) < 1e-6):
                otime = dtime.strftime('%Y-%m-%d %H:%M:%S MST')
                oval = f"{data['Value']:0.2f}"
        finally:
            out_dict['last_time'] = otime
            out_dict['last_value'] = oval
        out.append(out_dict)
    logger.info('Done updating data')
    return out


def update_existing_file(metadata_file, base_url, auth, params, timeout):
    logger.info('Updating values in metadata file')
    with open(metadata_file, 'r') as f:
        meta = json.load(f)

    out = get_latest_data(base_url, meta, auth=auth,
                          params=params, timeout=timeout)
    _, tmpfile = tempfile.mkstemp(dir=metadata_file.parent)
    try:
        p = Path(tmpfile)
        with open(p, "w") as f:
            json.dump(out, f)
    except Exception:
        p.unlink()
        raise ()
    else:
        p.chmod(0o644)
        p.rename(metadata_file)
