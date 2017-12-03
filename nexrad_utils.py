"""Create Hovmoeller plot of NEXRAD data."""

import os
import warnings
import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import s3fs

from datetime import datetime as dt
import matplotlib.pyplot as plt
import pyart


YMD = "%Y/%m/%d/"
KEY_START = ""
S3_FMT = 'noaa-nexrad-level2/%Y/%m/%d/{}/{}%Y%m%d_%H'
NEX_FMT = "{}%Y%m%d_%H%M%S_V06"

def get_s3_list_by_str(year, month, day, hhmm, radar_id, bucket='noaa-nexrad-level2'):
    """
    Return a list of files from AWS S3 bucket.

    Construct bucket name from input. Query the S3 bucket.

    Parameters
    ----------
    year: str
        4-digit year.
    month: str
        2-digit month.
    day: str
        2-digit day.
    hhmm: str
        Up to 4 digits corresponding to hours and minutes.
        None will drop this case for search.
    radar_id: str
        Valid ICAO radar name, e.g. KATX.
    bucket: str
        AWS bucket name to search non S3.
    engine: str
        What backend to use for querying S3 bucket.
        This was put in for the case that boto is deprecated.

    Returns
    -------
    List of keys for queried bucket.
    """
#    bucket_query = os.path.join(bucket, YMD, radar_id).strfmt(dt.year, dt.month, dt.day)
    bucket_query = '{}/{}/{}/{}/{}/{}{}{}{}'.format(
        bucket, year, month, day, radar_id, radar_id,
        year, month, day)
    if hhmm is not None:
        bucket_query += '_{}'.format(hhmm)

    # Create a connection with S3 server, without needed AWS credentials
    s3conn = s3fs.S3FileSystem(anon=True)
    key_list = s3conn.glob(bucket_query + '*')
    return key_list, bucket_query


def get_s3_list(start, end, radar_id, bucket='noaa-nexrad-level2'):
    """
    Return a list of files from AWS S3 bucket.

    Construct bucket name from input. Query the S3 bucket.

    Parameters
    ----------
    year: str
        4-digit year.
    month: str
        2-digit month.
    day: str
        2-digit day.
    hhmm: str
        Up to 4 digits corresponding to hours and minutes.
        None will drop this case for search.
    radar_id: str
        Valid ICAO radar name, e.g. KATX.
    bucket: str
        AWS bucket name to search non S3.
    engine: str
        What backend to use for querying S3 bucket.
        This was put in for the case that boto is deprecated.

    Returns
    -------
    List of keys for queried bucket.
    """
    key_list = []
    bucket_query = S3_FMT.format(radar_id, radar_id)
    dt_range = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq='H')
    # Create a connection with S3 server, without needed AWS credentials
    s3conn = s3fs.S3FileSystem(anon=True)

    for timeh in dt_range:
        key_list += s3conn.glob(timeh.strftime(bucket_query) + '*')
    return key_list


def open_nexrad_file(filename, io='radx'):
    """
    Open file using pyart nexrad archive method.

    Parameters
    ----------
    filename: str
        Radar filename to open.
    io: str
        Py-ART open method. If radx then file is opened via Radx
        otherwise via native Py-ART function.
    """
    filename, zipped = try_file_gunzip(filename)
    if io.lower() == 'radx':
        radar = pyart.aux_io.read_radx(filename)
    else:
        radar = pyart.io.read_nexrad_archive(filename)
    if zipped:
        os.system('gzip {}'.format(filename))
    return radar


def open_nexrad_from_s3(s3key, io='radx'):
    """
    Open file from S3 bucket.

    Parameters
    ----------
    s3key: str
        Full key of the file to be opened.
    io: str
        Py-ART open method. If radx then file is opened via Radx
        otherwise via native Py-ART function.
    """
    path, filename = os.path.split(s3key)
    with tempfile.TemporaryFile() as temp88d:
        s3fs.get(s3key, temp88d)
        return open_nexrad_file(temp88d, io=io)

def get_composite_field(radar, field, azimuth=235):
    """
    Produce a composite field from radar object and variable.
    """
    d = pyart.graph.RadarDisplay(radar)
    ray, x, y, z = d._get_azimuth_rhi_data_x_y_z('reflectivity', azimuth, True, None, True, None)
    
    ray[np.logical_or(ray > 1000., ray < -1000.)] = np.nan
    raycomp = np.nanmax(ray, axis=0)
    return raycomp, d.ranges


def get_composite_from_list(file_list, radar_id, field, azimuth):
    times = []
    rays = []
    ranges = []
    for filen in file_list:
        path, filename = os.path.split(filen)
        times.append(pd.to_datetime(os.path.basename(filename), format=NEX_FMT.format(radar_id)))
        path, filename = os.path.split(filen)
        r = pyart.io.read(filen)
        ray, rng = get_composite_field(r, field, azimuth=azimuth)
        rays.append(ray.data)
        ranges.append(rng)
    ds = xr.Dataset()
    ds['time'] = times
    ds['range'] =  ranges[0]
    ds['ref'] = ('time', 'range'), np.vstack(rays)
    return ds


def get_composite_from_s3_list(file_list, radar_id, field, azimuth):
    s3conn = s3fs.S3FileSystem(anon=True)
    times = []
    rays = []
    ranges = []
    for filen in file_list:
        path, filename = os.path.split(filen)
        with s3conn.open(filen, 'rb') as temp88d:
            try:
                r = pyart.io.read(temp88d)
                times.append(pd.to_datetime(os.path.basename(filename), format=NEX_FMT.format(radar_id)))
                ray, rng = get_composite_field(r, field, azimuth=azimuth)
                rays.append(ray.data)
                ranges.append(rng)
            except:
                pass
    ds = xr.Dataset()
    ds['time'] = times
    ds['range'] =  ranges[0]
    ds['ref'] = ('time', 'range'), np.vstack(rays)
    return ds
