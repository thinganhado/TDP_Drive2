from datetime import datetime, timedelta
from typing import List, Tuple
import googlemaps
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GOOGLE_MAP_API_KEY")

gmaps = googlemaps.Client(key=api_key)

def decode_polyline(polyline) -> list:
    """Decodes a Polyline string into a list of lat/lng dicts.

    See the developer docs for a detailed description of this encoding:
    https://developers.google.com/maps/documentation/utilities/polylinealgorithm

    :param polyline: An encoded polyline
    :type polyline: string

    :rtype: list of dicts with lat/lng keys
    """
    points = []
    index = lat = lng = 0

    while index < len(polyline):
        result = 1
        shift = 0
        while True:
            b = ord(polyline[index]) - 63 - 1
            index += 1
            result += b << shift
            shift += 5
            if b < 0x1f:
                break
        lat += (~result >> 1) if (result & 1) != 0 else (result >> 1)

        result = 1
        shift = 0
        while True:
            b = ord(polyline[index]) - 63 - 1
            index += 1
            result += b << shift
            shift += 5
            if b < 0x1f:
                break
        lng += ~(result >> 1) if (result & 1) != 0 else (result >> 1)

        points.append([lat * 1e-5, lng * 1e-5])

    return points

def get_directions_detail(start_location, end_location, start_time: datetime = datetime.now(), mode="driving"):
    # Request directions via the Google Maps API with a specified start time
    directions_result = gmaps.directions(
        start_location,
        end_location,
        mode=mode,
        departure_time=start_time
    )

    cur_time = start_time
    result_location = []
    overall_polyline = []
    estimated_end_time = start_time

    # Extract the polyline from the directions result
    for route in directions_result:
        for leg in route['legs']:
            for step in leg['steps']:
                overall_polyline.append(step['polyline']['points'])
                polyline = decode_polyline(step['polyline']['points'])
                duration = step['duration']['value']
                time_per_point = duration / (max(len(polyline), 1))
                for point in polyline:
                    result_location.append([point, cur_time])
                    cur_time += timedelta(seconds=time_per_point)
                estimated_end_time = cur_time

    return result_location, overall_polyline, estimated_end_time


def convert_to_next_weekday_time(hour, minute):
    # Get the current date and time
    now = datetime.now()

    # Calculate the number of days to add to get to the next weekday
    days_ahead = 1
    while (now + timedelta(days=days_ahead)).weekday() >= 5:  # 5 and 6 correspond to Saturday and Sunday
        days_ahead += 1

    # Create a datetime object for the next weekday with the given hour and minute
    next_weekday = now + timedelta(days=days_ahead)
    next_weekday_time = datetime.combine(next_weekday.date(), datetime.min.time()) + timedelta(hours=hour,
                                                                                               minutes=minute)

    return next_weekday_time




def encode_polyline(points: List[Tuple[float, float]]) -> str:
    """Encodes a list of lat/lng points into a polyline string.

    :param points: A list of tuples or lists containing latitude and longitude
    :type points: list of tuples/lists

    :rtype: string
    """
    result = []

    prev_lat = 0
    prev_lng = 0

    for lat, lng in points:
        lat = int(round(lat * 1e5))
        lng = int(round(lng * 1e5))

        d_lat = lat - prev_lat
        d_lng = lng - prev_lng

        prev_lat = lat
        prev_lng = lng

        for coord in [d_lat, d_lng]:
            coord = ~(coord << 1) if coord < 0 else (coord << 1)

            while coord >= 0x20:
                result.append(chr((0x20 | (coord & 0x1f)) + 63))
                coord >>= 5

            result.append(chr(coord + 63))

    return ''.join(result)


def combine_route_info(route_data):
    total_distance = 0
    total_duration = 0
    # combined_polyline =

    for leg in route_data[0]['legs']:
        total_distance += leg['distance']['value']
        total_duration += leg['duration']['value']
        # combined_polyline.append(leg['steps'])

    # Extract and combine the polylines
    overview_polyline = route_data[0]["overview_polyline"]["points"]

    return  total_distance,  total_duration,  overview_polyline

def get_route_info(start_location, end_location, start_time: datetime = datetime.now(), mode="driving"):
    # Request directions via the Google Maps API
    directions_result = gmaps.directions(start_location, end_location, mode=mode, departure_time=start_time)

    # Extract the route information
    total_distance, total_duration, overview_polyline = combine_route_info(directions_result)

    return total_distance, total_duration, overview_polyline


