from typing import Tuple

import cartopy.crs as ccrs
import s3fs
import xarray as xr


class HRRRGrid:
    """
    Helper class to convert coordinates based on the HRRR AWS Zarr grid.
    """
    FILE_SYSTEM = s3fs.S3FileSystem(anon=True)

    # HRRR base grid specs for zarr
    GRID_LON = 262.5
    GRID_LAT = 38.5
    GRID_SLAT = 38.5
    GRID_AXIS = 6371229

    # The HRRR data is stored with this grid
    CRS = ccrs.LambertConformal(
        central_longitude=GRID_LON,
        central_latitude=GRID_LAT,
        standard_parallels=(GRID_SLAT, GRID_SLAT),
        globe=ccrs.Globe(
            semimajor_axis=GRID_AXIS,
            semiminor_axis=GRID_AXIS
        )
    )

    # HRRR grid index to lookup chunk IDs and x/y chunk coordinates
    # for Lat/Lon points
    CHUNK_INDEX = xr.open_zarr(
        s3fs.S3Map("s3://hrrrzarr/grid/HRRR_chunk_index.zarr", s3=FILE_SYSTEM)
    )

    @staticmethod
    def transform_lon_lat(lon: float, lat:float) -> Tuple[float, float]:
        """
        Transform given coordinates into latitude and longitude of HRRR grid

        :param lon: Longitude of point
        :type lon: float
        :param lat: Latitude of point
        :type lat: float
        :return: Longitude and Latitude in HRRR CRS
        :rtype: Tuple[float, float]
        """
        return HRRRGrid.CRS.transform_point(
            lon, lat, ccrs.PlateCarree()
        )

    @staticmethod
    def chunk_info_for_lon_lat(lon: float, lat: float) -> xr.Dataset:
        """
        Retrieve HRRR tile chunk for given coordinates.

        :param lon: Longitude of point
        :type lon: float
        :param lat: Latitude of point
        :type lat: float

        :return: Dataset with closest matching grid point
        :rtype: xarray.Dataset
        """
        x, y = HRRRGrid.transform_lon_lat(lon, lat)

        return HRRRGrid.CHUNK_INDEX.sel(
            x=x, y=y, method="nearest"
        )
