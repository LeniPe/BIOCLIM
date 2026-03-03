import os
from dotenv import load_dotenv

from src.data_downloader import DEDLDownloader
from src.bioclim import temperature_raw_to_monthly, calc_bioclim, water_raw_to_monthly

load_dotenv()


def main(start_year=2022, end_year=2023):

    dir_monhtly = "./data/monthly"
    downloader = DEDLDownloader()
    for year in range(start_year, end_year + 1):
        if not os.path.exists(f"{dir_monhtly}/temperature_monthly_{year}.nc"):
            _ = downloader.download_year(
                year, "2m_temperature", "EO.ECMWF.DAT.ERA5_LAND_HOURLY"
            )
            temperature_raw_to_monthly(year)

        if not os.path.exists(f"{dir_monhtly}/water_monthly_{year}.nc"):
            _ = downloader.download_year(
                year, "volumetric_soil_water_layer_1", "EO.ECMWF.DAT.ERA5_LAND_MONTHLY"
            )
            _ = downloader.download_year(
                year, "volumetric_soil_water_layer_2", "EO.ECMWF.DAT.ERA5_LAND_MONTHLY"
            )
            water_raw_to_monthly(year)

    calc_bioclim()


if __name__ == "__main__":
    main()
