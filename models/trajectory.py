from pydantic import BaseModel, Field
from typing import Tuple, List, Optional, Any, Union
from datetime import datetime, time

class RequestMatchLocationBase(BaseModel):
    start_location: Tuple[float, float]
    end_location: Tuple[float, float]
    start_time:  time | datetime
    end_time: time | datetime


class RequestMatchLocation(RequestMatchLocationBase):
    pass

class RequestMatchLocationResponse(BaseModel):
    start_location: Tuple[float, float]
    end_location: Tuple[float, float]
    start_time: time
    end_time: time
    matches: List[int]


class RequestMatchBase(BaseModel):
    user_id: int

class RequestMatch(RequestMatchBase):
    pass

class RequestMatchResponse(RequestMatchBase):
    matches: List[int]


class PolylineRequestListBase(BaseModel):
    user_ids: List[int]

class PolylineRequestList(PolylineRequestListBase):
    pass

class PolylineItem(BaseModel):
    user_id: int
    polyline: str
    start_time: time
    end_time: time
class PolylineResponse(PolylineRequestList):
    polyline: List[PolylineItem] = Field(default_factory=list)


class UserMatchDetail(BaseModel):
    user_id: int
    name: str
    rating: int
    time_diff: float
    distance_diff: float
    polyline: str | None

class RequestMatchResponseDetail(RequestMatchBase):
    matches: List[UserMatchDetail] = Field(default_factory=list)

class TripItem(BaseModel):
    start_location: Union[str, Tuple[float, float]]
    end_location: Union[str, Tuple[float, float]]
    start_time: time
    end_time: time
    polyline: str
    distance: float
    duration: float

class TripRequest(BaseModel):
    driver_id: int
    rider_id: int
    driver_start_location: Union[str, Tuple[float, float]]
    driver_end_location: Union[str, Tuple[float, float]]
    rider_start_location: Union[str, Tuple[float, float]]
    rider_end_location: Union[str, Tuple[float, float]]
    driver_start_time: time | datetime

class TripRequestResponse(TripRequest):
    driver_end_time: time | datetime
    trips: List[TripItem] = Field(default_factory=list)

class TrajectoryCreateRequest(BaseModel):
    user_id: int

class TrajectoryCreateResponse(TrajectoryCreateRequest):
    user_id: int
    traj_id: int

class TripRequestV2(BaseModel):
    driver_id: int
    rider_id: int

class TripRequestResponseV2(TripRequestV2):
    driver_start_location: Union[str, Tuple[float, float]]
    driver_end_location: Union[str, Tuple[float, float]]
    rider_start_location: Union[str, Tuple[float, float]]
    rider_end_location: Union[str, Tuple[float, float]]
    driver_start_time: time | datetime
    driver_end_time: time | datetime
    trips: List[TripItem] = Field(default_factory=list)