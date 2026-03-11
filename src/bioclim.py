import os
import shutil
import tempfile
from netCDF4 import Dataset  # type: ignore

from dask import config as dask_config
import xarray as xr
import rioxarray  # noqa: F401
from dask.distributed import LocalCluster, Client
import glob


def _filter_valid_netcdf_files(
    file_paths: list[str],
    *,
    remove_invalid: bool = True,
    label: str = "files",
) -> list[str]:
    valid_files: list[str] = []
    invalid_files: list[str] = []

    for file_path in file_paths:
        try:
            with Dataset(file_path, mode="r") as dataset:
                if not dataset.variables:
                    raise ValueError("No variables found")
                first_var_name = next(iter(dataset.variables))
                first_var = dataset.variables[first_var_name]
                if first_var.ndim == 0:
                    first_var[...]
                else:
                    indexers = tuple(0 for _ in range(first_var.ndim))
                    first_var[indexers]
            valid_files.append(file_path)
        except Exception as exc:
            print(f"⚠️ Invalid NetCDF file skipped: {file_path} ({exc})")
            invalid_files.append(file_path)

    if remove_invalid:
        for invalid_file in invalid_files:
            try:
                os.remove(invalid_file)
                print(f"🗑️ Removed invalid NetCDF file: {invalid_file}")
            except OSError as exc:
                print(f"⚠️ Could not remove invalid file {invalid_file}: {exc}")

    if not valid_files:
        raise ValueError(f"No valid NetCDF {label} found after validation")

    return valid_files


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

    temp_files = sorted(glob.glob(f"{raw_dir}/2m_temperature_{year}_*.nc"))
    temp_files = _filter_valid_netcdf_files(
        temp_files,
        remove_invalid=True,
        label=f"temperature files for year {year}",
    )
    ds_temp = xr.open_mfdataset(
        temp_files,
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
    layer_1_file = f"{raw_dir}/volumetric_soil_water_layer_1_{year}.nc"
    layer_2_file = f"{raw_dir}/volumetric_soil_water_layer_2_{year}.nc"

    valid_layer_1_file = _filter_valid_netcdf_files(
        [layer_1_file],
        remove_invalid=True,
        label=f"soil water layer 1 file for year {year}",
    )[0]
    valid_layer_2_file = _filter_valid_netcdf_files(
        [layer_2_file],
        remove_invalid=True,
        label=f"soil water layer 2 file for year {year}",
    )[0]

    ds_1 = xr.open_dataset(valid_layer_1_file, engine="netcdf4")
    ds_2 = xr.open_dataset(valid_layer_2_file, engine="netcdf4")

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
    print(f"⏳ Building monthly climatology from {start_year} to {end_year}")
    temp_files = sorted(
        [
            f
            for f in glob.glob(f"{data_dir}/temperature_monthly_*.nc")
            if int(f.split("_")[-1].split(".")[0]) in range(start_year, end_year + 1)
        ]
    )
    temp_files = _filter_valid_netcdf_files(
        temp_files,
        remove_invalid=True,
        label="temperature monthly files",
    )
    ds_temp = xr.open_mfdataset(temp_files, engine="netcdf4")

    water_files = sorted(
        [
            f
            for f in glob.glob(f"{data_dir}/water_monthly_*.nc")
            if int(f.split("_")[-1].split(".")[0]) in range(start_year, end_year + 1)
        ]
    )
    water_files = _filter_valid_netcdf_files(
        water_files,
        remove_invalid=True,
        label="water monthly files",
    )
    ds_water = xr.open_mfdataset(water_files, engine="netcdf4")
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

    print("⏳ Computing BIOCLIM layers.")

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

    bio_ds.to_array().to_netcdf(out_file)
    print("✅ BIOCLIM layers saved to:", out_file)
