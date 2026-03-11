# BIOCLIM

BIOCLIM downloads ERA5-Land data from the Destination Earth Data Lake (DEDL), converts the raw downloads into monthly climate layers, builds a multi-year climatology, and derives the 19 standard BIOCLIM variables as NetCDF output.

## Overview

The current pipeline is driven by `main.py` and performs the following steps for a given year range:

1. Download hourly ERA5-Land `2m_temperature` data month by month.
2. Download monthly ERA5-Land `volumetric_soil_water_layer_1` and `volumetric_soil_water_layer_2` data per year.
3. Convert raw downloads into yearly monthly NetCDF files:
   - `temperature_monthly_<year>.nc`
   - `water_monthly_<year>.nc`
4. Build a monthly climatology across the selected time range.
5. Compute BIOCLIM variables `bio1` to `bio19` and write them to a NetCDF file.

The code currently uses:

- `xarray` and `netCDF4` for reading and writing NetCDF data
- `rioxarray` for CRS handling
- `dask.distributed` for optional local parallel processing during temperature aggregation
- `destinelab` for authenticated DEDL access

## Inputs and outputs

### Source variables

The pipeline currently requests these DEDL variables:

- `2m_temperature` from `EO.ECMWF.DAT.ERA5_LAND_HOURLY`
- `volumetric_soil_water_layer_1` from `EO.ECMWF.DAT.ERA5_LAND_MONTHLY`
- `volumetric_soil_water_layer_2` from `EO.ECMWF.DAT.ERA5_LAND_MONTHLY`

### Default directory layout

By default, `--data-dir` points to `./data`, and the pipeline creates:

- `data/raw/` – downloaded source NetCDF files
- `data/monthly/` – yearly monthly aggregates
- `data/climatology/` – climatology output
- `data/bioclim/` – final BIOCLIM layers

### Output files

For a run with `--start-year 2022 --end-year 2023`, the main outputs are:

- `data/monthly/temperature_monthly_2022.nc`
- `data/monthly/water_monthly_2022.nc`
- `data/monthly/temperature_monthly_2023.nc`
- `data/monthly/water_monthly_2023.nc`
- `data/climatology/climate_monthly_2022_2023.nc`
- `data/bioclim/BV_2022-2023.nc`

## Requirements

- Python 3.12
- `uv` for environment and dependency management
- Valid Destination Earth Data Lake credentials

If you run outside the container, some geospatial dependencies may also require system libraries such as GDAL.

## Setup

Install dependencies:

```bash
uv sync --locked
```

If you also want notebook support:

```bash
uv sync --locked --group notebooks
```

## Credentials

The downloader reads credentials from environment variables:

```bash
export DEDL_USER="your_username"
export DEDL_PWD="your_password"
```

You can also place them in a `.env` file in the repository root:

```env
DEDL_USER=your_username
DEDL_PWD=your_password
```

`main.py` loads `.env` automatically at startup.

## Run locally

Default run:

```bash
uv run main.py
```

Example for a custom year range:

```bash
uv run main.py --start-year 2021 --end-year 2023 --keep-raw false
```

## Command-line options

The current CLI supports these arguments:

- `--start-year`
- `--end-year`
- `--keep-raw`
- `--data-dir`
- `--dask-mode` (`local` or `none`)
- `--dask-workers`
- `--dask-threads-per-worker`
- `--dask-memory-limit`
- `--temp-time-chunk`
- `--temp-lat-chunk`
- `--temp-lon-chunk`

### Environment variable fallbacks

Each of the following environment variables is read by `main.py`:

- `BIOCLIM_START_YEAR` (default: `2022`)
- `BIOCLIM_END_YEAR` (default: `2023`)
- `BIOCLIM_KEEP_RAW` (default: `true`)
- `BIOCLIM_DATA_DIR` (default: `./data`)
- `BIOCLIM_DASK_MODE` (default: `local`)
- `BIOCLIM_DASK_WORKERS` (default: `4`)
- `BIOCLIM_DASK_THREADS_PER_WORKER` (default: `1`)
- `BIOCLIM_DASK_MEMORY_LIMIT` (default: `10GB`)
- `BIOCLIM_TEMP_TIME_CHUNK` (default: `48`)
- `BIOCLIM_TEMP_LAT_CHUNK` (default: `901`)
- `BIOCLIM_TEMP_LON_CHUNK` (default: `-1`)

## Notes on processing

- Invalid NetCDF downloads are detected and removed before processing.
- Temperature raw files are resampled to daily min, max, and mean values and then aggregated to monthly means.
- Soil water currently combines `swvl1` and `swvl2` into `W_mean`.
- The climatology is grouped by calendar month across the requested year range.
- The final BIOCLIM output is written from an `xarray.Dataset` converted to a NetCDF array.

## Docker

Build and run with Docker Compose:

```bash
docker compose up --build
```

The current Compose configuration:

- mounts `./data` into `/app/data`
- reads credentials from `.env`
- runs `uv run main.py --start-year 2022 --end-year 2023`

The container image entrypoint defined in the Dockerfile is also `uv run main.py`.

## Development

Run formatting and checks:

```bash
uv run ruff format
uv run ruff check .
uv run mypy src main.py
```
