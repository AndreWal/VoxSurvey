from pydantic import BaseModel, Field
from typing import Literal

class SurveyRow(BaseModel):
    age: int = Field(ge=18, le=100)
    gender: Literal[1, 2, 3] | None = None
    polint: Literal[1, 2, 3, 4] | None = None
    vote_id: int
