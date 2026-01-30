import pydantic
from pydantic import Field


class Record(pydantic.BaseModel):
    record_id: int
    transient_failure: bool = False
    permanent_failure: bool = False
    children: list["Record"] = Field(default_factory=list)

