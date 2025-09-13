from pydantic import BaseModel, Field
from typing import Optional

class Brewery(BaseModel):
    id: str
    name: Optional[str] = None
    brewery_type: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = Field(default=None)
    longitude: Optional[float] = Field(default=None)
