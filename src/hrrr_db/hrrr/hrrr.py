from datetime import timedelta

import dask
import dask.dataframe as dd
import numcodecs as ncd
import numpy as np
import pandas as pd
import s3fs
import xarray as xr

from .hrrr_grid import HRRRGrid
from .hrrr_info import PointChunkInfo, VariableInfo
from .hrrr_url import HRRRUrl


class HRRR:
    FILE_SYSTEM = s3fs.S3FileSystem(anon=True)

    def __init__(self, debug=False):
        self._debug = debug

    def load_dataset(self, variable):
        hrrr_url = HRRRUrl(variable, None)

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

    def load_tile(self, variable, forecast_hour=None):
        s3_url = HRRRUrl(variable, self._point_chunk_info).s3_chunk_url()

        if self._debug:
            print(s3_url)

        with self.FILE_SYSTEM.open(s3_url, 'rb') as compressed_data:
            buffer = ncd.blosc.decompress(compressed_data.read())

            dtype = "<f2"
            if "surface/PRES" in s3_url:
                dtype = "<f4"

            chunk = np.frombuffer(buffer, dtype=dtype)

            entry_size = 150*150
            num_entries = len(chunk)//entry_size

            if num_entries == 1:  # analysis file is 2d
                data_array = np.reshape(chunk, (150, 150))
            else:
                data_array = np.reshape(chunk, (num_entries, 150, 150))

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
            self, start_date, end_date, forecast_hour, variable
    ):
        """
        Dask data frame containing the values for the given variable in the
        requested date range
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
    def surface_variable_for_date(self, date, forecast_hour, variable):
        """
        Construct a pandas data frame using the date as index and the variable
        as value column. This method is designed for use as a Dask
        data frame with parallelized loading.
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

    def set_chunk_info_for_lon_lat(self, lon, lat):
        """
        Convert Lon/Lat into chunk coordinates
        """
        enclosing_tile = HRRRGrid.chunk_info_for_lon_lat(lon, lat)

        self._point_chunk_info = PointChunkInfo(
            x=int(enclosing_tile.in_chunk_x.values),
            y=int(enclosing_tile.in_chunk_y.values),
            chunk_id=enclosing_tile.chunk_id.values,
        )

        if self._debug:
            print(self._point_chunk_info)
