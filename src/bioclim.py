import os
import shutil
import tempfile

import xarray as xr
from dask.distributed import LocalCluster, Client


def temperature_raw_to_monthly(
    year: int, raw_dir: str = "./data/raw", out_dir: str = "./data/monthly"
):
    # Ensure output directory exists
    os.makedirs(out_dir, exist_ok=True)

    # --- Setup Dask cluster ---
    cluster = LocalCluster(
        n_workers=1,
        threads_per_worker=4,
        memory_limit="60GB",
    )
    _client = Client(cluster)

    print(f"Processing {year}")

    temp_file = f"{raw_dir}/2m_temperature_{year}_*.nc"
    ds_temp = xr.open_mfdataset(temp_file, chunks={}, parallel=False, engine="netcdf4")

    ds_temp = ds_temp.chunk(
        {
            "valid_time": -1,
            "latitude": -1,
            "longitude": 900,
        }
    )

    daily = ds_temp.t2m.resample(valid_time="1D")
    daily_min = daily.min()
    daily_max = daily.max()
    daily_mean = daily.mean()

    ds_daily = xr.Dataset(
        {"T_min": daily_min, "T_max": daily_max, "T_mean": daily_mean}
    )

    # monthly = ds_daily.groupby("valid_time.month").mean()

    monthly = ds_daily.resample(valid_time="1M").mean()
    monthly = monthly.rename({"valid_time": "time"})

    # --- Write to a temp file first ---
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".nc", dir=out_dir)
    os.close(tmp_fd)  # Close the file descriptor; xarray will handle writing

    monthly.to_netcdf(tmp_path)
    cluster.close()

    # --- Move temp file to final destination only on success ---
    final_path = f"{out_dir}/temperature_monthly_{year}.nc"
    shutil.move(tmp_path, final_path)
    print(f"Saved monthly file: {final_path}")


def water_raw_to_monthly(
    year: int, raw_dir: str = "./data/raw", out_dir: str = "./data/monthly"
):

    os.makedirs(out_dir, exist_ok=True)

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
    ds_water = ds_water.groupby("month").mean(dim="valid_time")
    print(ds_water)

    ds_water.to_netcdf(f"{out_dir}/water_monthly_{year}.nc")


def climatological_aggregate(
    data_dir: str = "./data/monthly", out_dir: str = "./data/climatology"
):
    os.makedirs(out_dir, exist_ok=True)
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

    clim = ds.groupby("month").mean()
    clim.to_netcdf(f"{out_dir}/climatology_monthly.nc")


def calc_bioclim(data_dir: str = "./data/climatology", out_dir: str = "./data/bioclim"):
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/bioclim.nc"
    ds = xr.open_dataset(f"{data_dir}/climatology_monthly.nc")

    quarter_index = ((ds.month - 1) // 3) + 1

    T_mean_quarter = (
        ds.T_mean.groupby(quarter_index).mean("month").rename({"month": "quarter"})
    )
    W_mean_quarter = (
        ds.W_mean.groupby(quarter_index).mean("month").rename({"month": "quarter"})
    )
    print(W_mean_quarter)

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

    # 1. valid land mask
    valid_mask = W_mean_quarter.notnull().any("quarter")

    # 2. compute indices safely
    wettest_q = W_mean_quarter.fillna(1).argmax("quarter")
    driest_q = W_mean_quarter.fillna(4).argmin("quarter")
    warmest_q = T_mean_quarter.fillna(1).argmax("quarter")
    coldest_q = T_mean_quarter.fillna(4).argmin("quarter")

    # # 3. Mask ocean pixels
    # wettest_q = wettest_q.where(valid_mask)
    # driest_q  = driest_q.where(valid_mask)
    # warmest_q = warmest_q.where(valid_mask)
    # coldest_q = coldest_q.where(valid_mask)

    # # 4. Compute the arrays as integers
    # wettest_q = wettest_q.compute().astype("Int32")  # note capital 'I' -> nullable int
    # driest_q  = driest_q.compute().astype("Int32")
    # warmest_q = warmest_q.compute().astype("Int32")
    # coldest_q = coldest_q.compute().astype("Int32")

    BIO8 = T_mean_quarter.isel(quarter=wettest_q).drop("quarter")
    BIO9 = T_mean_quarter.isel(quarter=driest_q).drop("quarter")
    BIO18 = W_mean_quarter.isel(quarter=warmest_q).drop("quarter")
    BIO19 = W_mean_quarter.isel(quarter=coldest_q).drop("quarter")

    # mask ocean pixels
    BIO8 = BIO8.where(valid_mask)
    BIO9 = BIO9.where(valid_mask)
    BIO18 = BIO18.where(valid_mask)
    BIO19 = BIO19.where(valid_mask)

    # wettest_q = W_mean_quarter.argmax("quarter")
    # driest_q = W_mean_quarter.argmin("quarter")
    # warmest_q = T_mean_quarter.argmax("quarter")
    # coldest_q = T_mean_quarter.argmin("quarter")

    # BIO8 = T_mean_quarter.isel(quarter=wettest_q)
    # BIO9 = T_mean_quarter.isel(quarter=driest_q)
    # BIO18 = W_mean_quarter.isel(quarter=warmest_q)
    # BIO19 = W_mean_quarter.isel(quarter=coldest_q)
    print(BIO8)
    print(BIO1)

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
    print(bio_ds.longitude.min(), bio_ds.longitude.max())

    bio_ds = bio_ds.assign_coords(
        longitude=(((bio_ds.longitude + 180) % 360) - 180),
    )
    bio_ds = bio_ds.sortby("longitude")
    print(bio_ds.longitude.min(), bio_ds.longitude.max())
    bio_ds.to_netcdf(
        out_file, encoding={v: {"zlib": True, "complevel": 5} for v in bio_ds.data_vars}
    )


if __name__ == "__main__":
    # temperature_raw_to_monthly(1985)
    # climatological_aggregate()
    calc_bioclim()
