import os
import shutil
import tempfile

from dask import config as dask_config
import xarray as xr
import rioxarray # noqa: F401
from dask.distributed import LocalCluster, Client


def convert_temperature_raw_to_monthly(
    year: int,
    raw_dir: str = "./data/raw",
    out_dir: str = "./data/monthly",
    dask_mode: str = "local",
    dask_workers: int = 1,
    dask_threads_per_worker: int = 4,
    dask_memory_limit: str = "60GB",
    temp_time_chunk: int = 24,
    temp_lat_chunk: int = 300,
    temp_lon_chunk: int = 300,
):

    cluster = None
    if dask_mode == "local":
        dask_config.set(
            {
                "distributed.worker.memory.target": 0.6,
                "distributed.worker.memory.spill": 0.7,
                "distributed.worker.memory.pause": 0.85,
                "distributed.worker.memory.terminate": 0.98,
            }
        )
        cluster = LocalCluster(
            n_workers=dask_workers,
            threads_per_worker=dask_threads_per_worker,
            memory_limit=dask_memory_limit,
        )
        _client = Client(cluster)

    print(f"Processing {year}")

    temp_file = f"{raw_dir}/2m_temperature_{year}_*.nc"
    ds_temp = xr.open_mfdataset(
        temp_file,
        chunks={
            "valid_time": temp_time_chunk,
            "latitude": temp_lat_chunk,
            "longitude": temp_lon_chunk,
        },
        parallel=False,
        engine="netcdf4",
        combine="by_coords",
    )

    daily = ds_temp.t2m.resample(valid_time="1D")
    daily_min = daily.min()
    daily_max = daily.max()
    daily_mean = daily.mean()

    ds_daily = xr.Dataset(
        {"T_min": daily_min, "T_max": daily_max, "T_mean": daily_mean}
    )

    # monthly = ds_daily.groupby("valid_time.month").mean()

    monthly = ds_daily.resample(valid_time="MS").mean()

    monthly = prune_dataset_metadata(monthly)

    # --- Write to a temp file first ---
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".nc", dir=out_dir)
    os.close(tmp_fd)  # Close the file descriptor; xarray will handle writing

    monthly.to_netcdf(tmp_path)
    if cluster is not None:
        cluster.close()

    # --- Move temp file to final destination only on success ---
    final_path = f"{out_dir}/temperature_monthly_{year}.nc"
    shutil.move(tmp_path, final_path)
    print(f"Saved monthly file: {final_path}")


def convert_soil_water_raw_to_monthly(
    year: int, raw_dir: str = "./data/raw", out_dir: str = "./data/monthly"
):

    ds_1 = xr.open_dataset(
        f"{raw_dir}/volumetric_soil_water_layer_1_{year}.nc", engine="netcdf4"
    )
    ds_2 = xr.open_dataset(
        f"{raw_dir}/volumetric_soil_water_layer_2_{year}.nc", engine="netcdf4"
    )

    ds_water = xr.Dataset(
        {
            "W_mean": ds_1["swvl1"] + ds_2["swvl2"],
        }
    )
    ds_water = ds_water.assign_coords(month=ds_water.valid_time.dt.month)
    ds_water = ds_water.resample(valid_time="MS").mean()

    ds_water = prune_dataset_metadata(ds_water)

    ds_water.to_netcdf(f"{out_dir}/water_monthly_{year}.nc")


def prune_dataset_metadata(
    ds: xr.Dataset,
    allowed_attrs: list[str] = ["units", "long_name"],
    allowed_coords: list[str] = ["valid_time", "latitude", "longitude"],
) -> xr.Dataset:
    ds.attrs = {}
    for var in ds.data_vars:
        ds[var].attrs = {k: v for k, v in ds[var].attrs.items() if k in allowed_attrs}

    drop_coords = [c for c in ds.coords if c not in allowed_coords]
    ds = ds.drop_vars(drop_coords)
    return ds


def build_monthly_climatology(
    data_dir: str = "./data/monthly",
    out_file: str = "./data/climatology/climate_monthly.nc",
    start_year: int = 1981,
    end_year: int = 2010,
):
    ds_temp = xr.open_mfdataset(
        f"{data_dir}/temperature_monthly_*.nc", engine="netcdf4"
    )
    ds_water = xr.open_mfdataset(f"{data_dir}/water_monthly_*.nc", engine="netcdf4")
    ds = xr.Dataset(
        {
            "T_mean": ds_temp.T_mean,
            "T_min": ds_temp.T_min,
            "T_max": ds_temp.T_max,
            "W_mean": ds_water.W_mean,
        }
    )

    clim = ds.groupby("valid_time.month").mean("valid_time")
    clim = prune_dataset_metadata(clim)

    # Add CRS information and reproject coordinates
    clim = clim.rio.write_crs("EPSG:4326")
    clim.coords["longitude"] = ((clim.longitude + 180) % 360) - 180
    clim = clim.sortby("longitude")

    clim.to_netcdf(out_file)


def compute_bioclim_layers(climatology_file: str, out_file: str):

    ds = xr.open_dataset(climatology_file, engine="netcdf4")

    quarter_index = ((ds.month - 1) // 3) + 1

    T_mean_quarter: xr.DataArray = (
        ds.T_mean.groupby(quarter_index).mean("month").rename({"month": "quarter"})
    )
    W_mean_quarter: xr.DataArray = (
        ds.W_mean.groupby(quarter_index).mean("month").rename({"month": "quarter"})
    )

    # BIO1
    BIO1 = ds.T_mean.mean("month")

    # BIO2
    BIO2 = (ds.T_max - ds.T_min).mean("month")

    # BIO4
    BIO4 = ds.T_mean.std("month") * 100

    # BIO5 & BIO6
    BIO5 = ds.T_max.max("month")
    BIO6 = ds.T_min.min("month")

    # BIO7
    BIO7 = BIO5 - BIO6

    # BIO3
    BIO3 = (BIO2 / BIO7) * 100

    # BIO10 & BIO11
    BIO10 = T_mean_quarter.max("quarter")
    BIO11 = T_mean_quarter.min("quarter")

    # BIO12
    BIO12 = ds.W_mean.mean("month")

    # BIO13 & BIO14
    BIO13 = ds.W_mean.max("month")
    BIO14 = ds.W_mean.min("month")

    # BIO15
    BIO15 = (ds.W_mean.std("month") / BIO12) * 100

    # BIO16 & BIO17
    BIO16 = W_mean_quarter.max("quarter")
    BIO17 = W_mean_quarter.min("quarter")

    valid_mask = W_mean_quarter.notnull().any("quarter")

    wettest_q = W_mean_quarter.fillna(1).argmax("quarter")
    driest_q = W_mean_quarter.fillna(4).argmin("quarter")
    warmest_q = T_mean_quarter.fillna(1).argmax("quarter")
    coldest_q = T_mean_quarter.fillna(4).argmin("quarter")

    BIO8 = T_mean_quarter.isel(quarter=wettest_q).drop_vars("quarter")
    BIO9 = T_mean_quarter.isel(quarter=driest_q).drop_vars("quarter")
    BIO18 = W_mean_quarter.isel(quarter=warmest_q).drop_vars("quarter")
    BIO19 = W_mean_quarter.isel(quarter=coldest_q).drop_vars("quarter")

    BIO8 = BIO8.where(valid_mask)
    BIO9 = BIO9.where(valid_mask)
    BIO18 = BIO18.where(valid_mask)
    BIO19 = BIO19.where(valid_mask)

    bio_ds = xr.Dataset(
        {
            "bio1": BIO1,
            "bio2": BIO2,
            "bio3": BIO3,
            "bio4": BIO4,
            "bio5": BIO5,
            "bio6": BIO6,
            "bio7": BIO7,
            "bio8": BIO8,
            "bio9": BIO9,
            "bio10": BIO10,
            "bio11": BIO11,
            "bio12": BIO12,
            "bio13": BIO13,
            "bio14": BIO14,
            "bio15": BIO15,
            "bio16": BIO16,
            "bio17": BIO17,
            "bio18": BIO18,
            "bio19": BIO19,
        }
    )

    bio_ds = bio_ds.rio.write_crs("EPSG:4326")
    bio_ds.to_netcdf(out_file)
