from datetime import datetime, timedelta

import dask
import dask.dataframe as dd
import numcodecs as ncd
import numpy as np
import numpy.typing as npt
import pandas as pd
import s3fs
import xarray as xr

from .hrrr_grid import HRRRGrid
from .hrrr_info import PointChunkInfo, VariableInfo
from .hrrr_url import HRRRUrl


class HRRR:
    """
    Interface to HRRR stored on AWS S3 as Zarr files.

    Much of this code credit goes to:
    https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/python_data_loading.html
    """
    FILE_SYSTEM = s3fs.S3FileSystem(anon=True)
    ZARR_CHUNK_SHAPE_X = 150
    ZARR_CHUNK_SHAPE_Y = 150

    def __init__(self, debug: bool = False):
        """
        :param debug: Print out debugging information. Default: False
        :type debug: bool
        """
        self._debug = debug
        self._point_chunk_info = None

    def load_dataset(self, variable: VariableInfo) -> xr.Dataset:
        """
        Get entire HRRR chunk for given variable.

        :param variable: Variable to get HRRR chunk for
        :type variable: VariableInfo

        :return: HRRR chunk for given variable
        :rtype: xr.Dataset
        """
        hrrr_url = HRRRUrl(variable)

        if self._debug:
            print(hrrr_url.s3_subgroup_url())

        urls = [
            s3fs.S3Map(hrrr_url.s3_group_url(), s3=self.FILE_SYSTEM),
            s3fs.S3Map(hrrr_url.s3_subgroup_url(), s3=self.FILE_SYSTEM)
        ]
        ds = xr.open_mfdataset(
            urls,
            engine='zarr',
            parallel=True,
        )

        # add the projection data
        ds = ds.rename(
            projection_x_coordinate="x", projection_y_coordinate="y"
        )
        ds = ds.metpy.assign_crs(HRRRGrid.CRS.to_cf())
        return ds.metpy.assign_latitude_longitude()

    def load_tile(
        self, variable: VariableInfo, forecast_hour: int = None
    ) -> npt.NDArray:
        """
        Load a tile directly from S3 for given variable.

        This method omits the use of Xarray and manually decompresses the
        Zarr data.

        :param variable: HRRR Variable to get
        :type variable: VariableInfo
        :param forecast_hour: HRRR forecast hour
        :type forecast_hour: int

        :return: HRRR variable values as array
        :rtype: npt.NDArray
        """
        s3_url = HRRRUrl(variable, self._point_chunk_info).s3_chunk_url()

        if self._debug:
            print(s3_url)

        with self.FILE_SYSTEM.open(s3_url, 'rb') as compressed_data:
            buffer = ncd.blosc.decompress(compressed_data.read())

            dtype = "<f2"
            if "surface/PRES" in s3_url:
                dtype = "<f4"

            chunk = np.frombuffer(buffer, dtype=dtype)

            entry_size = self.ZARR_CHUNK_SHAPE_Y * self.ZARR_CHUNK_SHAPE_X
            num_entries = len(chunk)//entry_size

            if num_entries == 1:  # analysis file is 2d
                data_array = np.reshape(
                    chunk,
                    (self.ZARR_CHUNK_SHAPE_Y, self.ZARR_CHUNK_SHAPE_X)
                )
            else:
                data_array = np.reshape(
                    chunk,
                    (
                        num_entries,
                        self.ZARR_CHUNK_SHAPE_Y,
                        self.ZARR_CHUNK_SHAPE_X
                    )
                )

        if forecast_hour is not None:
            # Translate forecast hour to array index (base 0)
            data_array = data_array[
                forecast_hour - 1,
                self._point_chunk_info.y,
                self._point_chunk_info.x
            ]
        else:
            data_array = data_array[
                :, self._point_chunk_info.y, self._point_chunk_info.x
            ]

        return data_array

    def surface_variable_for_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        forecast_hour: int,
        variable: str
    ) -> dask.dataframe.DataFrame:
        """
        Dask data frame variable values in the requested date range

        :param start_date: Start date
        :type start_date: datetime
        :param end_date: End date
        :type end_date: datetime
        :param forecast_hour: HRRR forecast hour
        :type forecast_hour: int
        :param variable: HRRR variable name
        :type variable: str

        :return: Dask dataframe with date as index
        :rtype: dask.dataframe.DataFrame
        """
        total_hours = (end_date - start_date).seconds // 3600
        date_range = [
            start_date + timedelta(hours=hour) for hour in range(total_hours)
        ]
        return dd.from_delayed(
            [
                self.surface_variable_for_date(date, forecast_hour, variable)
                for date in date_range
            ]
        ).compute()

    @dask.delayed
    def surface_variable_for_date(
        self, date: datetime, forecast_hour: int, variable: str
    ) -> dask.delayed:
        """
        Construct a pandas data frame using the date as index and the variable
        as value column. This method is designed for use as a Dask data frame
        with parallelized loading.

        :param date: Requested date
        :type date: datetime
        :param forecast_hour: HRRR forecast hour
        :type forecast_hour: int
        :param variable: HRRR variable name
        :type variable: str

        :return: Dask delayed function to load the pandas data frame.
        :rtype: dask.delayed
        """

        var_info = VariableInfo(
            run_hour=date,
            level='surface',
            name=variable
        )
        value = self.load_tile(var_info, forecast_hour=forecast_hour)
        forecast_date = date + timedelta(hours=forecast_hour)
        return pd.DataFrame(
            data=[value], index=[forecast_date], columns=[variable]
        )

    def set_chunk_info_for_lon_lat(self, lon: float, lat: float):
        """
        Convert given Lon/Lat into HRRR chunk coordinates

        This stores the chunk coordinates as an instance attribute.

        :param lon: Point longitude
        :type lon: float
        :param lat: Point Latitude
        :type lat: float
        """
        enclosing_tile = HRRRGrid.chunk_info_for_lon_lat(lon, lat)

        self._point_chunk_info = PointChunkInfo(
            x=int(enclosing_tile.in_chunk_x.values),
            y=int(enclosing_tile.in_chunk_y.values),
            chunk_id=enclosing_tile.chunk_id.values,
        )

        if self._debug:
            print(self._point_chunk_info)
