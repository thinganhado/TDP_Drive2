import numpy as np
import pandas as pd
from process.socialalgo.supabase_client import get_matched_user_ids, get_ratings_for_users, get_ratings_for_logged_in_user  # Import the required functions

# Function to calculate cosine similarity
def cosine_similarity(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    return dot_product / (norm_vec1 * norm_vec2)

# Function to calculate social similarity for a given user
def calculate_social_similarity(user_id):
    # Step 1: Get matched user IDs
    matched_user_ids = get_matched_user_ids(user_id)
    
    if not matched_user_ids:
        raise Exception("No matched users found.")
    
    # Step 2: Get ratings for the matched users
    ratings_data = get_ratings_for_users(matched_user_ids)
    
    # Step 3: Get the ratings for the logged-in user
    user_rating_data = get_ratings_for_logged_in_user(user_id)
    
    # Convert the ratings data into DataFrames for easier manipulation
    df_matched_users = pd.DataFrame(ratings_data)
    
    # Ensure the user_rating_data is in the same structure and convert it to a DataFrame
    df_logged_in_user = pd.DataFrame([user_rating_data])

    # Ensure the logged-in user's ratings are available
    if df_logged_in_user.empty:
        raise Exception(f"No ratings found for user with id {user_id}.")
    
    # Step 4: Extract the ratings vector for the logged-in user (excluding user_id)
    user_rating = df_logged_in_user.values.flatten()  # Exclude user_id
    
    # Step 5: Calculate cosine similarity for each matched user
    similarities = []
    for _, row in df_matched_users.iterrows():
        matched_user_rating = row.values  # Exclude user_id
        similarity_score = cosine_similarity(user_rating, matched_user_rating)
        similarities.append((row.iloc[-1], similarity_score))
    
    # Sort by highest similarity score
    similarities.sort(key=lambda x: x[1], reverse=True)
    
    return similarities  # Return list of (user_id, similarity_score) tuples