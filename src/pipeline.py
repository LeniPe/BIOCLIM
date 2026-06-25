import os

from src.data_downloader import DEDLDownloader
from src.bioclim import (
    convert_temperature_raw_to_monthly,
    convert_soil_water_raw_to_monthly,
)


def initialize_data_paths(data_dir: str, start_year: int, end_year: int):
    raw_dir = f"{data_dir}/raw"
    monthly_dir = f"{data_dir}/monthly"
    climatology_dir = f"{data_dir}/climatology"
    results_dir = f"{data_dir}/bioclim"

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(monthly_dir, exist_ok=True)
    os.makedirs(climatology_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    outfile = f"{results_dir}/BV_{start_year}-{end_year}.nc"
    climatology_file = f"{climatology_dir}/climate_monthly_{start_year}_{end_year}.nc"

    return raw_dir, monthly_dir, climatology_file, outfile


def download_and_prepare_monthly_data(
    start_year: int,
    end_year: int,
    raw_dir: str,
    monthly_dir: str,
    keep_raw: bool,
    force: bool,
    dask_mode: str,
    dask_workers: int,
    dask_threads_per_worker: int,
    dask_memory_limit: str,
    temp_time_chunk: int,
    temp_lat_chunk: int,
    temp_lon_chunk: int,
    poll_timeout_seconds: int,
) -> bool:
    downloader = DEDLDownloader()

    for year in range(start_year, end_year + 1):
        if not os.path.exists(f"{monthly_dir}/temperature_monthly_{year}.nc") or force:
            results = downloader.download_year(
                year,
                variable="2m_temperature",
                collection="EO.ECMWF.DAT.ERA5_LAND_HOURLY",
                poll_timeout_seconds=poll_timeout_seconds,
            )
            if not all(results):
                raise Exception(
                    f"Some temperature data for {year} failed to download. Check logs for details and try again later."
                )
            convert_temperature_raw_to_monthly(
                year,
                raw_dir=raw_dir,
                out_dir=monthly_dir,
                dask_mode=dask_mode,
                dask_workers=dask_workers,
                dask_threads_per_worker=dask_threads_per_worker,
                dask_memory_limit=dask_memory_limit,
                temp_time_chunk=temp_time_chunk,
                temp_lat_chunk=temp_lat_chunk,
                temp_lon_chunk=temp_lon_chunk,
            )
            if not keep_raw:
                for month in range(1, 13):
                    m_str = f"{month:02d}"
                    os.remove(f"{raw_dir}/2m_temperature_{year}_{m_str}.nc")

        if not os.path.exists(f"{monthly_dir}/water_monthly_{year}.nc") or force:
            results = downloader.download_year(
                year,
                variable="volumetric_soil_water_layer_1",
                collection="EO.ECMWF.DAT.ERA5_LAND_MONTHLY",
                poll_timeout_seconds=poll_timeout_seconds,
            )
            results += downloader.download_year(
                year,
                variable="volumetric_soil_water_layer_2",
                collection="EO.ECMWF.DAT.ERA5_LAND_MONTHLY",
                poll_timeout_seconds=poll_timeout_seconds,
            )
            if not all(results):
                raise Exception(
                    f"Some water data for {year} failed to download. Check logs for details and try again later."
                )
            convert_soil_water_raw_to_monthly(
                year, raw_dir=raw_dir, out_dir=monthly_dir
            )
            if not keep_raw:
                os.remove(f"{raw_dir}/volumetric_soil_water_layer_1_{year}.nc")
                os.remove(f"{raw_dir}/volumetric_soil_water_layer_2_{year}.nc")
