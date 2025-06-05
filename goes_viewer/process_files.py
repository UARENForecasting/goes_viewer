import boto3
from botocore import UNSIGNED
from botocore.client import Config
import cv2 as cv
import datetime as dt
import logging
import numpy as np
import os
from pathlib import Path
from PIL import Image
from PIL.PngImagePlugin import PngInfo
from pyproj import CRS, transform
import pyresample
import tempfile
import xarray as xr
import requests

from goes_viewer.config import CONTRAST
from goes_viewer.constants import (
    GOES_CORNERS,
    WEB_MERCATOR,
    DX,
    DY,
)

TEMP_STORAGE = os.getenv("TEMP_FILE", "outputs/")
logger = logging.getLogger(__name__)


def open_file(remote_file, corners):
    ds = xr.open_dataset(remote_file, engine='h5netcdf')
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
    """
    Uses simple fractional combination with gamma correction as described in
    https://doi.org/10.1029/2018EA000379
    """
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


def post_processing(image, kernel_size=(9,9), sigma=1.0, amount=1.25, threshold=0,
                    contrast=1.15, brightness=None):
    """
    Adjusts contrast and brightness of an uint8 image, then sharpens.
    contrast:   (0.0,  inf) with 1.0 leaving the contrast as is
    brightness: [-255, 255] with 0 leaving the brightness as is
    kernel_size: matrix size for gaussian blurring
    """
    if brightness is None:
        brightness = 160 - image.mean()
    brightness += int(round(255*(1-contrast)/2))
    brightened = cv.addWeighted(image, contrast, image, 0, brightness)
    # Use an unsharp mask with gaussian blurring
    blurred = cv.GaussianBlur(brightened, kernel_size, sigma)
    sharpened = float(amount + 1) * brightened - float(amount) * blurred
    sharpened = np.maximum(sharpened, np.zeros(sharpened.shape))
    sharpened = np.minimum(sharpened, 255 * np.ones(sharpened.shape))
    sharpened = sharpened.round().astype(np.uint8)
    if threshold > 0:
        low_contrast_mask = np.absolute(brightened - blurred) < threshold
        np.copyto(sharpened, brightened, where=low_contrast_mask)
    return sharpened


def make_img_filename(ds):
    ds_date = dt.datetime.utcfromtimestamp(ds.t.item() / 1e9)
    return f'{ds.platform_ID}_{ds_date.strftime("%Y-%m-%dT%H:%M:%SZ")}.png'


def save_local(img, filename, fig_dir, last_modified):
    """Save the goesviewer image, and add metadata text with last modified timestamp."""
    path = Path(fig_dir) / filename
    logger.info(f'Saving img to {path}')
    # make tempfile then move
    _, tmpfile = tempfile.mkstemp(dir=path.parent, prefix='zzztmp')
    try:
        metadata = PngInfo()
        metadata.add_text("last_modified", last_modified)
        tmp = Path(tmpfile)
        with open(tmp, mode='wb') as f:
            Image.fromarray(img).save(f, format="png", optimize=True,
                                      pnginfo=metadata)
    except Exception:
        tmp.unlink()
        raise
    else:
        tmp.chmod(0o644)
        tmp.rename(path)


def modify_prefix(base_prefix, prev=False):
    """Append the current time to the base S3 bucket prefix to get the latest data."""
    the_time = dt.datetime.now(tz=dt.timezone(dt.timedelta(0)))
    if prev:
        the_time = the_time - dt.timedelta(hours=1)
    
    return f"{base_prefix}/{the_time.year}/{the_time.strftime('%j')}/{the_time.hour:02}"


def process_s3_file(bucket, key):
    """Pull the noaa goes file from AWS, reshape, update contrast, and return to save as png.
    
    Parameters
    ----------
    bucket: str, The S3 bucket that the data is saved at. Typically "noaa-goes19"
    key: The datatype to pull from the bucket. We use "ABI-L2-MCMIPC"

    Returns
    -------
    final_img: np.array of the image data
    filename: name to save the file as, pulled from data
    """
    path = f'{bucket}/{key}'
    logger.info(f"Processing file {path}")
    corners = GOES_CORNERS
    # Example of why full_path needs to be made manually:
    # https://noaa-goes18.s3.amazonaws.com/ABI-L2-MCMIPC/2025/143/19/OR_ABI-L2-MCMIPC-M6_G18_s20251431926191_e20251431928564_c20251431929241.nc
    # noaa-goes18/ABI-L2-MCMIPC/2025/143/19/OR_ABI-L2-MCMIPC-M6_G18_s20251431936191_e20251431938564_c20251431939242.nc
    full_path = f"https://{bucket}.s3.amazonaws.com{path.split(bucket)[1]}"
    resp = requests.get(full_path)
    resp.raise_for_status()
    if not os.path.isdir(TEMP_STORAGE):
        os.mkdir(TEMP_STORAGE)
    filepath = os.path.join(TEMP_STORAGE, "temp.nc")
    with open(filepath, "wb") as file:
        file.write(resp.content)
    with open(filepath, "rb") as remote_file:
        # Open with xarray
        ds = open_file(remote_file, corners)
        # Grab RGB from the file
        img = make_geocolor_image(ds)
        # Resample and reshape image for AZ/NM
        resample_params, shape = make_resample_params(ds, GOES_CORNERS)
        resampled_img = resample_image(resample_params, shape, img)
        # Sharpen and increase contrast for prettier picture
        final_img = post_processing(resampled_img)
        # Translate ds metadata to filename
        filename = make_img_filename(ds)
    os.unlink(filepath)
    return final_img, filename


def check_and_save_recent_files(bucket_name, prefix, fig_dir):
    logger.debug("=== Starting run ===")
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects")
    full_prefix = modify_prefix(prefix)
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=full_prefix)
    page = [x for x in page_iterator][-1]
    while 'Contents' not in page.keys():
        full_prefix = modify_prefix(prefix, prev=True)
        logger.debug(f"No files created in bucket yet, checking previous folder: {full_prefix}")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=full_prefix)
        page = [x for x in page_iterator][-1]

    for obj in page['Contents']:
        already_saved = False
        last_modified = obj['LastModified']
        last_modified = dt.datetime.strftime(last_modified, "%d_%m_%y-%H:%M:%S")
        logger.debug(f"Checking file: {obj['Key']}")
        # Check that the file isn't being worked on, if so go to the next
        lockfile_path = os.path.join(fig_dir, last_modified + ".tmp")
        if os.path.exists(lockfile_path):
            logger.debug("File being processed, skipping")
            continue
        tmp_file = open(lockfile_path, 'w')
        tmp_file.close()
        # Check that file hasn't already been finished. Only check 
        # 12 most recent files
        all_imgs = sorted(os.listdir(fig_dir))
        for img_saved in all_imgs[::-1][:12]:
            if '.png' not in img_saved:
                continue
            filepath = os.path.join(fig_dir, img_saved)
            with Image.open(filepath) as target_img:
                if target_img.text['last_modified'] == last_modified:
                    logging.debug(f"File already processed. Skipping.")
                    already_saved = True
        if not already_saved:
            img, filename = process_s3_file(bucket_name, obj['Key'])
            save_local(img, filename, fig_dir, last_modified)
        os.remove(lockfile_path)
    logger.debug("=== Run finished ===")


def remove_old_files(save_directory, keep_from=24):
    latest = dt.datetime.now() - dt.timedelta(hours=keep_from)
    for file_ in save_directory.glob('*.png'):
        try:
            if '_' not in file_.stem:
                raise ValueError
            file_time = dt.datetime.strptime(
                file_.stem.split('_')[1], '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            logger.warning('File %s has an invalid time format', file_)
            continue
        else:
            if file_time < latest:
                logger.info('Removing file %s', file_)
                file_.unlink()
