import os
import argparse
from dotenv import load_dotenv
from src.bioclim import build_monthly_climatology, compute_bioclim_layers
from src.pipeline import download_and_prepare_monthly_data, initialize_data_paths


def main(
    start_year: int = 2022,
    end_year: int = 2023,
    keep_raw: bool = True,
    force: bool = False,
    data_dir: str = "./data",
    dask_mode: str = "local",
    dask_workers: int = 1,
    dask_threads_per_worker: int = 4,
    dask_memory_limit: str = "60GB",
    temp_time_chunk: int = 24,
    temp_lat_chunk: int = 300,
    temp_lon_chunk: int = 300,
    poll_timeout_seconds: int = 30 * 60,
):
    load_dotenv()
    raw_dir, monthly_dir, climatology_file, outfile = initialize_data_paths(
        data_dir, start_year, end_year
    )

    if os.path.exists(outfile) and not force:
        print("✅ BIOCLIM layers already exist. Skipping download and processing.")
        return

    download_and_prepare_monthly_data(
        start_year=start_year,
        end_year=end_year,
        raw_dir=raw_dir,
        monthly_dir=monthly_dir,
        keep_raw=keep_raw,
        force=force,
        dask_mode=dask_mode,
        dask_workers=dask_workers,
        dask_threads_per_worker=dask_threads_per_worker,
        dask_memory_limit=dask_memory_limit,
        temp_time_chunk=temp_time_chunk,
        temp_lat_chunk=temp_lat_chunk,
        temp_lon_chunk=temp_lon_chunk,
        poll_timeout_seconds=poll_timeout_seconds,
    )

    build_monthly_climatology(
        data_dir=monthly_dir,
        out_file=climatology_file,
        start_year=start_year,
        end_year=end_year,
    )
    compute_bioclim_layers(climatology_file=climatology_file, out_file=outfile)


def str_to_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download ERA5-Land data and compute BIOCLIM layers"
    )
    parser.add_argument(
        "--start-year", type=int, default=int(os.getenv("BIOCLIM_START_YEAR", "2022"))
    )
    parser.add_argument(
        "--end-year", type=int, default=int(os.getenv("BIOCLIM_END_YEAR", "2023"))
    )
    parser.add_argument(
        "--keep-raw",
        type=str_to_bool,
        default=str_to_bool(os.getenv("BIOCLIM_KEEP_RAW", "true")),
    )
    parser.add_argument("--data-dir", default=os.getenv("BIOCLIM_DATA_DIR", "./data"))
    parser.add_argument(
        "--dask-mode",
        choices=["local", "none"],
        default=os.getenv("BIOCLIM_DASK_MODE", "local"),
    )
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
    parser.add_argument(
        "--poll-timeout-seconds",
        type=int,
        default=int(os.getenv("BIOCLIM_POLL_TIMEOUT_SECONDS", str(30 * 60))),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        start_year=args.start_year,
        end_year=args.end_year,
        keep_raw=args.keep_raw,
        data_dir=args.data_dir,
        dask_mode=args.dask_mode,
        dask_workers=args.dask_workers,
        dask_threads_per_worker=args.dask_threads_per_worker,
        dask_memory_limit=args.dask_memory_limit,
        temp_time_chunk=args.temp_time_chunk,
        temp_lat_chunk=args.temp_lat_chunk,
        temp_lon_chunk=args.temp_lon_chunk,
        poll_timeout_seconds=args.poll_timeout_seconds,
    )
