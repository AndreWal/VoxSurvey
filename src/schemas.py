from pydantic import BaseModel, Field

class SurveyRow(BaseModel):
    vote_id: str = Field(min_length=1)
    title: str
    vote_date: str
    language: str