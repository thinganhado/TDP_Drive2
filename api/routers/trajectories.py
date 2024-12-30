from fastapi import APIRouter, HTTPException
from models.trajectory import (RequestMatch, RequestMatchResponse, RequestMatchLocation, RequestMatchLocationResponse
, PolylineRequestList, PolylineResponse, RequestMatchResponseDetail, TripRequestResponse, TripRequest, TrajectoryCreateRequest, TrajectoryCreateResponse
                               , TripRequestV2, TripRequestResponseV2)
from crud.trajectory import (match_trajectory_by_location, match_trajectory_by_user as db_match_trajectory_by_user
, get_polyline_users as db_get_polyline_users, match_trajectory_by_user_detail as db_match_trajectory_by_user_detail
, get_trip_info as db_get_trip_info
, create_trajectory_by_id as db_create_trajectory
,get_trip_info_v2 as db_get_trip_info_v2)

router = APIRouter()

@router.post("/match/location", response_model=RequestMatchLocationResponse)
def match_trajectory(request: RequestMatchLocation):
    """
    Match trajectories based on start and end location
    """
    # Implement your logic to match trajectories here
    # For now, let's return a dummy response
    response = match_trajectory_by_location(request)
    return response

@router.post("/match/user", response_model=RequestMatchResponse)
def match_trajectory_by_user(request: RequestMatch):
    """
    Match trajectories based on user id
    """
    response = db_match_trajectory_by_user(request)
    return response

@router.post("/match/polyline", response_model=PolylineResponse)
def get_polyline(request: PolylineRequestList):
    """
    Get the polyline of the user based on the user idt:
    """
    response = db_get_polyline_users(request)
    return response

@router.post("/match/create_match")
def match_with_user():
    pass


@router.post("/match/user/detail", response_model=RequestMatchResponseDetail)
def match_trajectory_by_user_detail(request: RequestMatch):
    """
    Get the details of the matched users
    """
    response = db_match_trajectory_by_user_detail(request)
    return response

@router.post("/match/trip/info", response_model=TripRequestResponse)
def get_trip_info(request: TripRequest):
    """
    Get specific trip information based on driver, rider starting/ending location and time
    """
    response = db_get_trip_info(request)
    return response

@router.post("/traj/create", response_model=TrajectoryCreateResponse)
def create_trajectory_by_user_id(request: TrajectoryCreateRequest):
    """
    Create a new trajectory based on the user id. Only create if traj does not exist
    """
    response = db_create_trajectory(request)
    return response

@router.post("/trip/info/v2", response_model=TripRequestResponseV2)
def get_trip_info_v2(request: TripRequestV2):
    """
    Create a new trajectory based on the user id. Only create if traj does not exist
    """
    response = db_get_trip_info_v2(request)

    return response