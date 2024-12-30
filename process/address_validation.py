import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from db_manager import db  # Use db instance to interact with the database
from fastapi import HTTPException, status


# Function to identify home and work locations using DBSCAN clustering
def identify_home_work_locations(coordinates_df, eps=0.01, min_samples=2):
    user_data = coordinates_df.copy()

    # Check if there's enough data for clustering
    if user_data.shape[0] < min_samples:
        return None, None

    # Extract latitude and longitude as NumPy array
    coords = user_data[['latitude', 'longitude']].values

    # Apply DBSCAN clustering
    kms_per_radian = 6371.0088  # Earth's radius in kilometers
    epsilon = eps / kms_per_radian  # Convert `eps` from kilometers to radians
    db = DBSCAN(eps=epsilon, min_samples=min_samples, metric='haversine').fit(np.radians(coords))

    # Add cluster labels to the DataFrame
    user_data['cluster'] = db.labels_

    # Identify clusters and their sizes
    cluster_sizes = user_data['cluster'].value_counts()

    # Check if enough clusters were found (at least 2 clusters for home and work)
    if len(cluster_sizes) < 2:
        return None, None

    # Identify clusters with the most points
    home_cluster = cluster_sizes.idxmax()  # Largest cluster (most frequent location) is home
    work_cluster = cluster_sizes[cluster_sizes.index != home_cluster].idxmax()  # Second largest is work

    # Calculate the mean latitude and longitude for home and work locations
    home_coords = user_data[user_data['cluster'] == home_cluster][['latitude', 'longitude']].mean().values
    work_coords = user_data[user_data['cluster'] == work_cluster][['latitude', 'longitude']].mean().values

    return home_coords, work_coords


# Function to calculate the great-circle distance between two points
def are_locations_within(coords1, coords2, max_distance_meters=500):
    location1 = (coords1[0], coords1[1])
    location2 = (coords2[0], coords2[1])

    # Calculate the great-circle distance between the two points in kilometers
    distance_km = great_circle(location1, location2).kilometers

    # Convert kilometers to meters and compare
    distance_meters = distance_km * 1000

    return distance_meters <= max_distance_meters


# Function to validate home and work addresses against the actual data
def validate_address(user_id):
    """
    Fetch data from the database instead of an Excel file. Modify this function to query the database 
    and retrieve the actual home and work coordinates for the user.
    """

    # Query the database for coordinates data
    select_query = "SELECT latitude, longitude, time_stamp FROM public.location_signal WHERE user_id = %s"
    coordinates_df = pd.DataFrame(db.fetch_all(select_query, (user_id,)), columns=['latitude', 'longitude', 'timestamp'])

    # If there's no data for this user, return HTTP 404
    if coordinates_df.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No data found for user {user_id}")

    # Query actual home and work coordinates from the database
    actual_coords_query = "SELECT home_latitude, home_longitude, work_latitude, work_longitude FROM public.users WHERE user_id = %s"
    actual_home_work_coords = db.fetch_all(actual_coords_query, (user_id,))

    if not actual_home_work_coords:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No actual home or work locations found for user {user_id}")

    actual_home_coords = actual_home_work_coords[0][:2]  # Home latitude and longitude
    actual_work_coords = actual_home_work_coords[0][2:]  # Work latitude and longitude

    # Generating home and work coordinates from the trip data
    generated_home_coords, generated_work_coords = identify_home_work_locations(coordinates_df)

    if generated_home_coords is None or generated_work_coords is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Could not generate home or work coordinates.")

    # Validating home coordinates
    if are_locations_within(actual_home_coords, generated_home_coords):
        home_validation_message = f"Home coordinates for user {user_id}: {actual_home_coords} validated successfully."
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Home coordinates do not match.")

    # Validating work coordinates
    if are_locations_within(actual_work_coords, generated_work_coords):
        work_validation_message = f"Work coordinates for user {user_id}: {actual_work_coords} validated successfully."
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Work coordinates do not match.")

    return {"status": "success", "home_message": home_validation_message, "work_message": work_validation_message}
