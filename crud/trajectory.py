from models.trajectory import (RequestMatchLocationResponse, RequestMatchLocation, RequestMatchResponse, RequestMatch, PolylineResponse,
                               PolylineRequestList, PolylineItem, RequestMatchResponseDetail, UserMatchDetail, TripRequestResponse, TripRequest, TripItem,
                               TrajectoryCreateRequest, TrajectoryCreateResponse, TripRequestV2, TripRequestResponseV2)
from process.trajectories.matching_ver2 import search_potential_similar_trajectories, search_potential_similar_trajectories_by_user
from process.trajectories.db_op import get_user_polyline, calculate_trajectory_by_id
from process.trajectories.gmapfunction import get_route_info
from crud.user import  check_user_exists, get_user_by_id, get_user_trip
from fastapi import  HTTPException
from datetime import datetime, time, timedelta
from database import Database

def match_trajectory_by_location(request: RequestMatchLocation):
    # Implement your logic to match trajectories here
    # For now, let's return a dummy response
    # if not check_user_exists(request.user_id):
    #     raise HTTPException(status_code=404, detail="User not found")
    df = search_potential_similar_trajectories(request.start_location[0], request.start_location[1], request.end_location[0], request.end_location[1], request.start_time, request.end_time, level =14, time_diff=20)
    df = df[df["start_user_id"] != request.user_id]
    user_ids = df["start_user_id"].tolist()

    response = RequestMatchLocationResponse(
        start_location=request.start_location,
        end_location=request.end_location,
        start_time=request.start_time,
        end_time=request.end_time,
        matches=user_ids  # Dummy match IDs
    )
    return response


def match_trajectory_by_user(request: RequestMatch):
    # if not check_user_exists(request.user_id):
    #     raise HTTPException(status_code=404, detail="User not found")
    df =  search_potential_similar_trajectories_by_user(request.user_id, level =14, time_diff=20)
    print("Search potential similar trajectories by user completed")
    df = df[df["start_user_id"] != request.user_id]
    user_ids = df["start_user_id"].tolist()

    response = RequestMatchResponse(
        user_id=request.user_id,
        matches=user_ids  # Dummy match IDs
    )
    return response

def get_polyline_users(request: PolylineRequestList):
    # Initialize the response with an empty list for polyline
    response = PolylineResponse(
        user_ids=request.user_ids,
        polyline=[]  # Initialize with an empty list
    )

    for user_id in request.user_ids:
        # if not check_user_exists(user_id):
        #     raise HTTPException(status_code=404, detail="User not found")
        poly = get_user_polyline(user_id)
        response.polyline.append(PolylineItem(user_id=user_id, polyline=poly, start_time="00:00:00", end_time="00:00:00"))

    return response


def match_trajectory_by_user_detail(request: RequestMatch):
    user_match_details = []
    user_match = []
    df =  search_potential_similar_trajectories_by_user(request.user_id, level =14, time_diff=20)
    print("Search potential similar trajectories by user completed")
    df = df[df["start_user_id"] != request.user_id]

    for index, row in df.iterrows():
        name = "Something"
        poly = get_user_polyline(row['start_user_id'])
        user_match_detail = UserMatchDetail(
            user_id=row['start_user_id'],
            name=name,
            rating= 5,
            time_diff=row['start_time_diff']+row['end_time_diff'],
            distance_diff=row['score'],
            polyline=poly
        )
        user_match_details.append(user_match_detail)
        user_match.append((request.user_id, row['start_user_id'], int(row['score']), int(row['start_time_diff']+row['end_time_diff']), poly))

    add_custom_match(user_match)

    response = RequestMatchResponseDetail(
        user_id=request.user_id,
        matches=user_match_details  # Dummy match IDs
    )
    return response


def get_trip_info(request: TripRequest):
    trip_items = []
    if isinstance(request.driver_start_time, datetime):
        time_part = request.driver_start_time.time()
    else:
        time_part = request.driver_start_time

    tomorrow_date = datetime.now().date() + timedelta(days=1)
    tomorrow_datetime = datetime.combine(tomorrow_date, time_part)

    pickup_distance,  pickup_time, pickup_polyline =  get_route_info(request.driver_start_location, request.rider_start_location, tomorrow_datetime)

    cur_time = tomorrow_datetime + timedelta(seconds=pickup_time)
    trip_items.append(TripItem(
        start_location=request.driver_start_location,
        end_location=request.rider_start_location,
        start_time=request.driver_start_time,
        end_time=cur_time.time(),
        polyline=pickup_polyline,
        distance=pickup_distance,
        duration=pickup_time
    ))


    dropoff_distance, dropoff_time, dropoff_polyline = get_route_info(request.rider_start_location, request.rider_end_location, cur_time)

    trip_items.append(TripItem(
        start_location=request.rider_start_location,
        end_location=request.rider_end_location,
        start_time=cur_time.time(),
        end_time=(cur_time + timedelta(seconds=dropoff_time)).time(),
        polyline=dropoff_polyline,
        distance=dropoff_distance,
        duration=dropoff_time))

    cur_time += timedelta(seconds=dropoff_time)

    finish_distance, finish_time, finish_polyline = get_route_info(request.rider_end_location, request.driver_end_location, cur_time)
    trip_items.append(TripItem(
        start_location=request.rider_end_location,
        end_location=request.driver_end_location,
        start_time=cur_time.time(),
        end_time=(cur_time + timedelta(seconds=finish_time)).time(),
        polyline=finish_polyline,
        distance=finish_distance,
        duration=finish_time))
    cur_time += timedelta(seconds=finish_time)





    response = TripRequestResponse(
        **request.model_dump(),
        driver_end_time=cur_time.time(),
        trips=trip_items
    )
    return response


def add_custom_match(custom_matches):
    sql = """
    INSERT INTO custom_match_test (rider_id, driver_id, distance_diff, time_diff,polyline)  
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (rider_id, driver_id) DO NOTHING
    """

    db = Database()
    db.connect()
    db.execute_many(sql, custom_matches)
    db.close_connection()

def create_trajectory_by_id(request: TrajectoryCreateRequest):
    if not check_user_exists(request.user_id):
        raise HTTPException(status_code=404, detail="User not found")

    res = get_traj_by_user(request.user_id)
    if not res:
        print("Populate trajectory by id {}".format(request.user_id))
        calculate_trajectory_by_id(request.user_id)
        print("Populate trajectory by id completed")
        res = get_traj_by_user(request.user_id)


    return TrajectoryCreateResponse(user_id=request.user_id, traj_id=res[0][0])

def get_traj_by_user(user_id: int):
    db = Database()
    db.connect()
    sql = """
        select traj_id
        from trajectory
        where user_id = %s
        LIMIT 1
    """
    res = db.fetch_all(sql, [user_id])
    db.close_connection()
    return res


def get_trip_info_v2(request: TripRequestV2):
    trip_items = []

    driver_info = get_user_trip(request.driver_id)
    rider_info = get_user_trip(request.rider_id)
    if not driver_info or not rider_info:
        raise HTTPException(status_code=404, detail="User not found")

    driver_start_location = (driver_info[0], driver_info[1])
    driver_end_location = (driver_info[2], driver_info[3])
    rider_start_location = (rider_info[0], rider_info[1])
    rider_end_location = (rider_info[2], rider_info[3])
    driver_start_time = driver_info[4]


    if isinstance(driver_start_time, datetime):
        time_part = driver_start_time.time()
    else:
        time_part =driver_start_time

    tomorrow_date = datetime.now().date() + timedelta(days=1)
    tomorrow_datetime = datetime.combine(tomorrow_date, time_part)

    pickup_distance,  pickup_time, pickup_polyline =  get_route_info(driver_start_location, rider_start_location, tomorrow_datetime)

    cur_time = tomorrow_datetime + timedelta(seconds=pickup_time)

    trip_items.append(TripItem(
        start_location=driver_start_location,
        end_location=rider_start_location,
        start_time=driver_start_time,
        end_time=cur_time.time(),
        polyline=pickup_polyline,
        distance=pickup_distance,
        duration=pickup_time
    ))

    dropoff_distance, dropoff_time, dropoff_polyline = get_route_info(rider_start_location, rider_end_location, cur_time)

    trip_items.append(TripItem(
        start_location=rider_start_location,
        end_location=rider_end_location,
        start_time=cur_time.time(),
        end_time=(cur_time + timedelta(seconds=dropoff_time)).time(),
        polyline=dropoff_polyline,
        distance=dropoff_distance,
        duration=dropoff_time))

    cur_time += timedelta(seconds=dropoff_time)

    finish_distance, finish_time, finish_polyline = get_route_info(rider_end_location, driver_end_location, cur_time)

    trip_items.append(TripItem(
        start_location=rider_end_location,
        end_location=driver_end_location,
        start_time=cur_time.time(),
        end_time=(cur_time + timedelta(seconds=finish_time)).time(),
        polyline=finish_polyline,
        distance=finish_distance,
        duration=finish_time))
    cur_time += timedelta(seconds=finish_time)

    response = TripRequestResponseV2(
        **request.model_dump(),
        driver_start_location=driver_start_location,
        driver_end_location=driver_end_location,
        rider_start_location=rider_start_location,
        rider_end_location=rider_end_location,
        driver_start_time=driver_start_time,
        driver_end_time=cur_time.time(),
        trips=trip_items
    )
    return response









