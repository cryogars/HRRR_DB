import dataclasses

from .hrrr_info import PointChunkInfo, VariableInfo


@dataclasses.dataclass
class HRRRUrl:
    variable: VariableInfo
    point_info: PointChunkInfo

    S3_BASE = "s3://hrrrzarr/"

    def s3_group_url(self, xr_open=False):
        url = self.S3_BASE if xr_open else ""  # Add when using Xarray

        return self.variable.run_hour.strftime(
            f"{url}hrrrzarr/sfc/%Y%m%d/%Y%m%d_%Hz_fcst.zarr/"
            f"{self.variable.level}/{self.variable.name}"
        )

    def s3_subgroup_url(self, xr_open=False):
        return f"{self.s3_group_url(xr_open)}/{self.variable.level}"

    def s3_chunk_url(self):
        return (f"{self.s3_subgroup_url()}/"
                f"{self.variable.name}/"
                f"{self.point_info.chunk_id}")
