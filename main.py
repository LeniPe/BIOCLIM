import os
import argparse
from dotenv import load_dotenv

from src.data_downloader import DEDLDownloader
from src.bioclim import climatological_aggregate, temperature_raw_to_monthly, calc_bioclim, water_raw_to_monthly




def main(
    start_year: int = 2023,
    end_year: int = 2023,
    keep_raw: bool = True,
    raw_dir: str = "./data/raw",
    monthly_dir: str = "./data/monthly",
    climatology_dir: str = "./data/climatology",
    bioclim_dir: str = "./data/bioclim",
    dask_mode: str = "local",
    dask_workers: int = 1,
    dask_threads_per_worker: int = 4,
    dask_memory_limit: str = "60GB",
    temp_time_chunk: int = 24,
    temp_lat_chunk: int = 300,
    temp_lon_chunk: int = 300,
):
    load_dotenv()

    downloader = DEDLDownloader()
    for year in range(start_year, end_year + 1):
        if not os.path.exists(f"{monthly_dir}/temperature_monthly_{year}.nc"):
            results = downloader.download_year(
                year, "2m_temperature", "EO.ECMWF.DAT.ERA5_LAND_HOURLY"
            )
            if not all(results):
                print(f"⚠️ Some temperature data for {year} failed to download. Check logs for details.")
                return
            temperature_raw_to_monthly(
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

        if not os.path.exists(f"{monthly_dir}/water_monthly_{year}.nc"):
            results = downloader.download_year(
                year, "volumetric_soil_water_layer_1", "EO.ECMWF.DAT.ERA5_LAND_MONTHLY"
            )
            results += downloader.download_year(
                year, "volumetric_soil_water_layer_2", "EO.ECMWF.DAT.ERA5_LAND_MONTHLY"
            )
            if not all(results):
                print(f"⚠️ Some water data for {year} failed to download. Check logs for details.")
                return
            water_raw_to_monthly(year, raw_dir=raw_dir, out_dir=monthly_dir)
            if not keep_raw:
                os.remove(f"{raw_dir}/volumetric_soil_water_layer_1_{year}.nc")
                os.remove(f"{raw_dir}/volumetric_soil_water_layer_2_{year}.nc")

    climatological_aggregate(data_dir=monthly_dir, out_dir=climatology_dir)
    calc_bioclim(data_dir=climatology_dir, out_dir=bioclim_dir)


def str_to_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download ERA5-Land data and compute BIOCLIM layers")
    parser.add_argument("--start-year", type=int, default=int(os.getenv("BIOCLIM_START_YEAR", "2023")))
    parser.add_argument("--end-year", type=int, default=int(os.getenv("BIOCLIM_END_YEAR", "2023")))
    parser.add_argument(
        "--keep-raw",
        type=str_to_bool,
        default=str_to_bool(os.getenv("BIOCLIM_KEEP_RAW", "true")),
    )
    parser.add_argument("--raw-dir", default=os.getenv("BIOCLIM_RAW_DIR", "./data/raw"))
    parser.add_argument("--monthly-dir", default=os.getenv("BIOCLIM_MONTHLY_DIR", "./data/monthly"))
    parser.add_argument(
        "--climatology-dir", default=os.getenv("BIOCLIM_CLIMATOLOGY_DIR", "./data/climatology")
    )
    parser.add_argument("--bioclim-dir", default=os.getenv("BIOCLIM_BIOCLIM_DIR", "./data/bioclim"))
    parser.add_argument("--dask-mode", choices=["local", "none"], default=os.getenv("BIOCLIM_DASK_MODE", "local"))
    parser.add_argument(
        "--dask-workers", type=int, default=int(os.getenv("BIOCLIM_DASK_WORKERS", "4"))
    )
    parser.add_argument(
        "--dask-threads-per-worker",
        type=int,
        default=int(os.getenv("BIOCLIM_DASK_THREADS_PER_WORKER", "1")),
    )
    parser.add_argument(
        "--dask-memory-limit",
        default=os.getenv("BIOCLIM_DASK_MEMORY_LIMIT", "10GB"),
    )
    parser.add_argument(
        "--temp-time-chunk",
        type=int,
        default=int(os.getenv("BIOCLIM_TEMP_TIME_CHUNK", "48")),
    )
    parser.add_argument(
        "--temp-lat-chunk",
        type=int,
        default=int(os.getenv("BIOCLIM_TEMP_LAT_CHUNK", "901")),
    )
    parser.add_argument(
        "--temp-lon-chunk",
        type=int,
        default=int(os.getenv("BIOCLIM_TEMP_LON_CHUNK", "-1")),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        start_year=args.start_year,
        end_year=args.end_year,
        keep_raw=args.keep_raw,
        raw_dir=args.raw_dir,
        monthly_dir=args.monthly_dir,
        climatology_dir=args.climatology_dir,
        bioclim_dir=args.bioclim_dir,
        dask_mode=args.dask_mode,
        dask_workers=args.dask_workers,
        dask_threads_per_worker=args.dask_threads_per_worker,
        dask_memory_limit=args.dask_memory_limit,
        temp_time_chunk=args.temp_time_chunk,
        temp_lat_chunk=args.temp_lat_chunk,
        temp_lon_chunk=args.temp_lon_chunk,
    )
