import dataclasses

from .hrrr_info import PointChunkInfo, VariableInfo


@dataclasses.dataclass
class HRRRUrl:
    """
    Construct the HRRR URL to fetch Zarr chunks from AWS
    """
    variable: VariableInfo
    point_info: PointChunkInfo = None

    S3_BASE = "s3://hrrrzarr/"

    def s3_group_url(self, xr_open: bool = False) -> str:
        """
        :param xr_open: Indicate if URL is opened with Xarray
        :type xr_open: bool
        """
        url = self.S3_BASE if xr_open else ""  # Add when using Xarray

        return self.variable.run_hour.strftime(
            f"{url}hrrrzarr/sfc/%Y%m%d/%Y%m%d_%Hz_fcst.zarr/"
            f"{self.variable.level}/{self.variable.name}"
        )

    def s3_subgroup_url(self, xr_open: bool = False) -> str:
        """
        :param xr_open: Indicate if URL is opened with Xarray
        :type xr_open: bool
        """
        return f"{self.s3_group_url(xr_open)}/{self.variable.level}"

    def s3_chunk_url(self) -> str:
        return (f"{self.s3_subgroup_url()}/"
                f"{self.variable.name}/"
                f"{self.point_info.chunk_id}")
