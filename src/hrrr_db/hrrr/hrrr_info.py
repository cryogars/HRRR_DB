import dataclasses

from datetime import datetime


@dataclasses.dataclass
class VariableInfo:
    run_hour: datetime
    level: str
    name: str


@dataclasses.dataclass
class PointChunkInfo:
    x: int
    y: int
    chunk_id: str

    def __post_init__(self):
        # S3 buckets have an added 0 to indicate extra time dimensions
        self.chunk_id = f"0.{self.chunk_id}"
