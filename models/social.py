from pydantic import BaseModel, Field
from typing import Any
class SocialSimRequest(BaseModel):
    user_id: int

class SocialSimResponse(BaseModel):
    matched_user_ids: list[int]
    ratings: list[Any] =  Field(default_factory=list)

class SocialRatingItem(BaseModel):
    user_id: int
    rating: float

class SocialRatingResponse(SocialSimRequest):
    ratings: list[SocialRatingItem] = Field(default_factory=list)
