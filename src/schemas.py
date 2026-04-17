from pydantic import BaseModel, Field
from typing import Literal, Annotated

class SurveyRow(BaseModel):
    age: Annotated[int, Field(ge=18, le=100)]
    gender: Annotated[Literal[1, 2, 3] | None, Field(default=None)]
    polint: Annotated[Literal[1, 2, 3, 4] | None, Field(default=None)]
    lrsp: Annotated[int | None, Field(ge=0, le=10, default=None)]
    vote: Annotated[Literal[1, 2, 3] | None, Field(default=None)]
    vote_id: int
    coderesp: int
