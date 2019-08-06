import datetime as dt
import json


import boto3
import numpy as np
from PIL import Image
from pyproj import CRS, transform
import pyresample
import s3fs
import xarray as xr


from goes_viewer.config import S3_PREFIX, CONTRAST
from goes_viewer.constants import (
    G16_CORNERS,
    G17_CORNERS,
    WEB_MERCATOR,
    DX,
    DY,
)


def open_file(path, corners, engine='h5netcdf'):
    ds = xr.open_dataset(path, engine=engine)
    proj_info = ds.goes_imager_projection
    proj4_params = {
        "ellps": "WGS84",
        "a": proj_info.semi_major_axis.item(),
        "b": proj_info.semi_minor_axis.item(),
        "rf": proj_info.inverse_flattening.item(),
        "proj": "geos",
        "lon_0": proj_info.longitude_of_projection_origin.item(),
        "lat_0": 0.0,
        "h": proj_info.perspective_point_height.item(),
        "x_0": 0,
        "y_0": 0,
        "units": "m",
        "sweep": proj_info.sweep_angle_axis,
    }
    crs = CRS.from_dict(proj4_params)
    bnds = transform(crs.geodetic_crs, crs, corners[:, 0], corners[:, 1])
    ds = ds.update(
        {
            "x": ds.x * proj_info.perspective_point_height,
            "y": ds.y * proj_info.perspective_point_height,
        }
    ).assign_attrs(crs=crs, proj4_params=proj4_params)

    xarg = np.nonzero(((ds.x >= bnds[0].min()) & (ds.x <= bnds[0].max())).values)[0]
    yarg = np.nonzero(((ds.y >= bnds[1].min()) & (ds.y <= bnds[1].max())).values)[0]
    return ds.isel(x=xarg, y=yarg)


def make_geocolor_image(ds):
    # Load the three channels into appropriate R, G, and B
    R = ds["CMI_C02"].data
    NIR = ds["CMI_C03"].data
    B = ds["CMI_C01"].data

    # Apply range limits for each channel. RGB values must be between 0 and 1
    R = np.clip(R, 0, 1)
    NIR = np.clip(NIR, 0, 1)
    B = np.clip(B, 0, 1)

    # Calculate the "True" Green
    G = 0.45 * R + 0.1 * NIR + 0.45 * B
    G = np.clip(G, 0, 1)

    # Apply the gamma correction
    gamma = 1 / 1.7
    R = np.power(R, gamma)
    G = np.power(G, gamma)
    B = np.power(B, gamma)

    cleanIR = ds["CMI_C13"].data
    ir_range = ds.max_brightness_temperature_C13.valid_range

    cleanIR = (cleanIR - ir_range[0]) / (ir_range[1] - ir_range[0])
    cleanIR = np.clip(cleanIR, 0, 1)
    cleanIR = 1 - cleanIR

    # Lessen the brightness of the coldest clouds so they don't appear so bright
    # when we overlay it on the true color image.
    cleanIR = cleanIR / 1.3

    # Maximize the RGB values between the True Color Image and Clean IR image
    RGB_ColorIR = np.dstack(
        [np.maximum(R, cleanIR), np.maximum(G, cleanIR), np.maximum(B, cleanIR)]
    )

    F = (259 * (CONTRAST + 255)) / (255.0 * 259 - CONTRAST)
    out = F * (RGB_ColorIR - 0.5) + 0.5
    out = np.clip(out, 0, 1)  # Force value limits 0 through 1.
    return out


def make_resample_params(ds, corners):
    # can be saved for later use to save time
    goes_area = pyresample.AreaDefinition(
        ds.platform_ID,
        "goes area",
        "goes-r",
        projection=ds.proj4_params,
        width=len(ds.x),
        height=len(ds.y),
        area_extent=(
            ds.x.min().item(),
            ds.y.min().item(),
            ds.x.max().item(),
            ds.y.max().item(),
        ),
    )
    pts = transform(
        ds.crs.geodetic_crs, WEB_MERCATOR, sorted(corners[:, 0]), sorted(corners[:, 1])
    )
    width = int((pts[0][1] - pts[0][0]) / DX)
    height = int((pts[1][1] - pts[1][0]) / DY)
    webm_area = pyresample.AreaDefinition(
        "webm",
        "web  mercator",
        "webm",
        projection=WEB_MERCATOR.to_proj4(),
        width=width,
        height=height,
        area_extent=(pts[0][0], pts[1][0], pts[0][1], pts[1][1]),
    )
    shape = (height, width)
    return (
        pyresample.bilinear.get_bil_info(goes_area, webm_area, 6e3, neighbours=8),
        shape,
    )


def resample_image(resample_params, shape, img_arr):
    out = np.dstack(
        [
            pyresample.bilinear.get_sample_from_bil_info(
                img_arr[..., i].reshape(-1), *resample_params, shape
            )
            for i in range(3)
        ]
        + [np.ones(shape)]
    )
    return (np.ma.fix_invalid(out).filled(0) * 255).astype("uint8")


def make_img_filename(ds):
    date = dt.datetime.utcfromtimestamp(ds.t.item() / 1e9)
    return f'{ds.platform_ID}_{date.strftime("%Y-%m-%dT%H:%M:%SZ")}.png'


def get_s3_keys(bucket, timestamp=None, prefix="ABL-L2-MCMIPF"):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    """
    s3 = boto3.client("s3")
    kwargs = {"Bucket": bucket}

    kwargs["Prefix"] = prefix
    if timestamp is not None:
        kwargs['Prefix'] += timestamp.strftime('%Y/%j/%H')

    while True:
        resp = s3.list_objects_v2(**kwargs)
        if resp["KeyCount"] == 0:
            break
        for obj in resp["Contents"]:
            key = obj["Key"]
            if key.startswith(prefix):
                yield key

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break


def save_s3(img, filename, save_bucket):
    fs = s3fs.S3FileSystem()
    with fs.open(f'{save_bucket}/{filename}', mode='wb') as f:
        Image.fromarray(img).save(f, format="png", optimize=True)


def process_s3_file(bucket, key):
    fs = s3fs.S3FileSystem(anon=True)
    remote_file = fs.open(f'{bucket}/{key}', mode='rb')
    if 'goes17' in bucket:
        corners = G17_CORNERS
    else:
        corners = G16_CORNERS
    ds = open_file(remote_file, corners, 'h5netcdf')
    img = make_geocolor_image(ds)
    resample_params, shape = make_resample_params(ds, G17_CORNERS)
    nimg = resample_image(resample_params, shape, img)
    return nimg, make_img_filename(ds)


def process_sqs_event(event, context):
    for erec in event['Records']:
        sns_msg = json.loads(erec['body'])
        rec = json.loads(sns_msg['Message'])
        for record in rec['Records']:
            print(record)
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            print(bucket, key)
            if key.startswith(S3_PREFIX):
                print(f'processing {key}')
                img, filename = process_s3_file(bucket, key)
                save_s3(img, filename, 'goes17images')

if __name__ == "__main__":
    import logging
    logging.basicConfig(format='%(asctime)s %(message)s', level='INFO')
    bucket = 'noaa-goes17'
    testmsg = {'messageId': 'c331898f-4088-4059-9181-e69f2a7f340a', 'receiptHandle': 'AQEBYdDuNgcX7hvQTbPngVf2oUJgVR/C6Azh9pAwUveEBEf7+7uEFTkk6/pa6honFw7w5xF5jo31i3imGJidCi4GFgtqBi574a1xAmCwNTHD3C9VhmbgtsfGQoYQLLtllW8GgX4Pjva78y/2tE/R/DYQVtBddNcY6KQ7Gd/crArtJy8sWLQgUtaziyoc8NhaZBvQqMWl+FtGEIIIw3SS45dG2KcUAPS40R02SbBc8DVyM/b3P6F3fDbhQtyfycVH800eY3BvlfGPzZaR4ffiu840Dym+vyvJ+QsHbjSp9zt+NvCJvCqzQnTpZfiMpw+4HPDXfadF+Ef6ePvsBxH6zyR992bdjXa0FoOFzmVhZw73RoQdvOUbrR2Pdmoh1OaTW6nBJ56q502ZDiyDnh3+ze1ejA==', 'body': '{\n "Type" : "Notification",\n "MessageId" : "191c04d9-d428-5417-b457-511b08af29c6",\n "TopicArn" : "arn:aws:sns:us-east-1:123901341784:NewGOES17Object",\n "Subject" : "Amazon S3 Notification",\n "Message" : "{\\"Records\\":[{\\"eventVersion\\":\\"2.1\\",\\"eventSource\\":\\"aws:s3\\",\\"awsRegion\\":\\"us-east-1\\",\\"eventTime\\":\\"2019-08-06T01:39:17.337Z\\",\\"eventName\\":\\"ObjectCreated:Put\\",\\"userIdentity\\":{\\"principalId\\":\\"AWS:AIDAJIC4U5R2TXT7T3MI6\\"},\\"requestParameters\\":{\\"sourceIPAddress\\":\\"198.85.226.62\\"},\\"responseElements\\":{\\"x-amz-request-id\\":\\"EBD615B128A3EBBB\\",\\"x-amz-id-2\\":\\"4CFJrIHl+bAyhktScnPO98mksJVAybhnIgA5FTXFrblFAON++7MuXBUZRq1wwZR80eic+RWd9mc=\\"},\\"s3\\":{\\"s3SchemaVersion\\":\\"1.0\\",\\"configurationId\\":\\"MjQ5OTllZTctZGNmZC00NjZkLThlNDctYmQxZTQzNDViZGU0\\",\\"bucket\\":{\\"name\\":\\"noaa-goes17\\",\\"ownerIdentity\\":{\\"principalId\\":\\"A2AJV00K47QOI1\\"},\\"arn\\":\\"arn:aws:s3:::noaa-goes17\\"},\\"object\\":{\\"key\\":\\"ABI-L1b-RadM/2019/218/01/OR_ABI-L1b-RadM2-M6C03_G17_s20192180138575_e20192180139033_c20192180139080.nc\\",\\"size\\":1063054,\\"eTag\\":\\"7239138a465f83a247851a1a64ac8927\\",\\"sequencer\\":\\"005D48DA450E238E88\\"}}}]}",\n "Timestamp" : "2019-08-06T01:39:19.120Z",\n "SignatureVersion" : "1",\n "Signature" : "DrjMXAKufoJ7lM2ZC7uZ5q0NwrGCoy4YQAT/Taef5uL+vXBVEyna1xRUMY9ImZHtjnV+l/qPtySUb7hghqM43HSVCqsX8+nE7LuTARLHcE8gNRi3Df9k+LF2a1E2PlbxVYFDWbm8Z/nxOvXE3MS1QiLKft1YJsHaJSnvwA5OzfAPzr633/X6PMm/KDdICFg6U6+t9zRc1hkn491lkefWy9LaNrTZJbFahsngrv9m2YlR1g4LF+MnozYjPkSclE9HA+1AkkJzKFNw9i53wDHSPGsZo+41w/0HXRUKuifCrcn+KKTdyDF25AvJoEh9OYd/t1sCJ2507Jujx+VL9rT4Eg==",\n "SigningCertURL" : "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-6aad65c2f9911b05cd53efda11f913f9.pem",\n "UnsubscribeURL" : "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:123901341784:NewGOES17Object:77b066df-2229-4e12-9086-6b092f7b2bc4"\n}', 'attributes': {'ApproximateReceiveCount': '1', 'SentTimestamp': '1565055559207', 'SenderId': 'AIDAIT2UOQQY3AUEKVGXU', 'ApproximateFirstReceiveTimestamp': '1565055679347'}, 'messageAttributes': {}, 'md5OfBody': '0f9ccabd35419d916abe50611f746af2', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:us-west-2:823337348524:goes17images', 'awsRegion': 'us-west-2'}
    snsjson = {"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2019-08-05T21:49:59.156Z","eventName":"ObjectCreated:CompleteMultipartUpload","userIdentity":{"principalId":"AWS:AIDAJIC4U5R2TXT7T3MI6"},"requestParameters":{"sourceIPAddress":"198.85.226.62"},"responseElements":{"x-amz-request-id":"C3F494D4C7CE757D","x-amz-id-2":"kWWHynZxH8bVDcuVDvruNGAKR9+hHAYoXOo5CoBGYai8c4KB2vrf6SyEWxrmkghy23BIhDFUfSg="},"s3":{"s3SchemaVersion":"1.0","configurationId":"MjQ5OTllZTctZGNmZC00NjZkLThlNDctYmQxZTQzNDViZGU0","bucket":{"name":"noaa-goes17","ownerIdentity":{"principalId":"A2AJV00K47QOI1"},"arn":"arn:aws:s3:::noaa-goes17"},"object":{"key":"ABI-L2-MCMIPF/2019/216/17/OR_ABI-L2-MCMIPF-M6_G17_s20192161730341_e20192161739408_c20192161739497.nc","size":24209676,"eTag":"92917931abf93d1adfe0f1d8dce3b293-3","sequencer":"005D48A4848C315273"}}}]}

    keys = get_s3_keys(bucket, prefix='ABI-L2-MCMIPF/2019/217')
    for key in keys:
        logging.info(key)
        img, filename = process_s3_file(bucket, key)
        with open(f'figs/{filename}', mode='wb') as f:
            Image.fromarray(img).save(f, format="png", optimize=True)
    # goes_dir = Path("/storage/projects/goes_alg/full_disk")
    # goes_files = sorted(list(goes_dir.glob("*L2-MC*.nc")), reverse=True)
    # for goes_file in goes_files:
    #     ds = open_file(goes_file, G17_CORNERS)
    #     img = make_geocolor_image(ds)

    #     resample_params, shape = make_resample_params(ds, G17_CORNERS)
    #     nimg = resample_image(resample_params, shape, img)
    #     Image.fromarray(nimg).save(make_img_filename(ds), format="png", optimize=True)
