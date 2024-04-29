"""
Helper dataclasses to hold HRRR variable and Zarr chunk info.
"""

import dataclasses

from datetime import datetime


@dataclasses.dataclass
class VariableInfo:
    """
    HRRR variable of interest
    """
    run_hour: datetime
    level: str
    name: str


@dataclasses.dataclass
class PointChunkInfo:
    """
    AWS HRRR grid chunk information
    See map on:
        https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/python_data_loading.html
    """
    x: int
    y: int
    chunk_id: str

    def __post_init__(self):
        # S3 buckets have an added 0 to indicate extra time dimensions
        self.chunk_id = f"0.{self.chunk_id}"
