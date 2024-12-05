import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray
import dask
from dask.distributed import Client
from utils import clip_dataset

def gen_wget_strings(start_year: int, end_year: int, var: str) -> list:
    """
    Access the UHE-daily gridded data product that formed the basis for
    Tuholske et al (2021).
    """
    daterange = pd.date_range(f"{start_year}-01-01", f"{end_year}-12-31")
    
    base_urls = {
        "himax": "https://data.chc.ucsb.edu/people/cascade/UHE-daily/himax",
        "Tmax": "http://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/global_cogs_p05/Tmax",
        "Tmin": "http://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/global_cogs_p05/Tmin"
    }

    base_url = base_urls[var]
    file_type = 'tif' if var == "himax" else 'cog'

    return [
        (
            date.strftime("%Y.%m.%d"),
            f"{base_url}/{date.strftime('%Y')}/{var}.{date.strftime('%Y.%m.%d')}.{file_type}",
        )
        for date in daterange
    ]

def add_time_dim(xda):
    """
    Extract datetime from tiff encoding and create time dimension when fed
    into mfdataset.
    """
    xda = xda.expand_dims(
        time=[np.datetime64("-".join(xda.encoding["source"].split(".")[-4:-1]))]
    )
    return xda

def load_uhedaily_ds(start_year, end_year, bounds, var, fpath):
    """
    Load in the UHE-daily dataset.
    """
    strings = gen_wget_strings(start_year, end_year, var)
    subset = [i[1] for i in strings]
    ds = (
        xr.open_mfdataset(
            subset,
            engine="rasterio",
            chunks={},
            parallel=True,
            concat_dim="time",
            combine="nested",
            preprocess=add_time_dim,
        )
        .squeeze(dim=["band"], drop=True)
        .drop("spatial_ref")
        .rename({"band_data": var, "x": "lon", "y": "lat"})
        .sortby("lat")
    )

    ds = clip_dataset(ds, bounds, adj=False)
    return ds.to_zarr(fpath)

def clip_dataset(ds, bounds, adj=False): 
    # Load the NetCDF file
    if adj:
        ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180)).sortby('lon')
   
    #minx, miny, maxx, maxy = bounds.total_bounds
    centroid = bounds.union_all().centroid
    x, y = centroid.x, centroid.y

    ds = ds.sel(lat=centroid.y, lon=centroid.x, method='nearest')
    return ds

bounds = gpd.read_file('bounds.geojson')

client = Client()
fpath = 'data/chirts_himax_historical.zarr'
load_uhedaily_ds(1985, 2014, bounds, 'himax', fpath)

