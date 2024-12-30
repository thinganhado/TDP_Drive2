from database import Database

# Query Supabase to get user data based on user_id
def get_user_data(user_id):
    db = Database()
    db.connect()
    select_query = """SELECT home_latitude, home_longitude, work_latitude, work_longitude, departure_time 
    FROM public.users 
    WHERE user_id = %s"""

    res = db.fetch_one(select_query, (user_id,))
    db.close_connection()
    print("get_user_data()")
    if not res:
        raise Exception(f"User with id {user_id} not found.")
    return res

# Query Supabase to get matched user IDs for the logged-in user
def get_matched_user_ids(user_id):
    db = Database()
    db.connect()
    select_query_driver = """
    SELECT rider_id 
    FROM public.custom_match_test 
    WHERE driver_id = %s
    
    UNION 
    
    SELECT driver_id 
    FROM public.custom_match_test 
    WHERE rider_id = %s
    """
    res = db.fetch_all(select_query_driver, (user_id, user_id))
    db.close_connection()
    print("get_matched_user_ids()")
    if not res:
        raise Exception(f"Match for user_id {user_id} not found.")

    return [row[0] for row in res]

# Query Supabase to get ratings data for matched user IDs
def get_ratings_for_users(user_ids):
    db = Database()
    db.connect()
    select_query = """
    SELECT chattiness, safety, punctuality, friendliness, comfortability, user_id 
    FROM public.ratings 
    WHERE user_id IN %s"""
    user_ids = tuple(user_ids)
    res = db.fetch_all(select_query, (user_ids,))
    db.close_connection()
    print("get_ratings_for_users")
    if not res:
        raise Exception("No ratings found for the matched users.")
    return res


# New Function: Query Supabase to get ratings for the logged-in user
def get_ratings_for_logged_in_user(user_id):
    db = Database()
    db.connect()
    select_query = """
    SELECT chattiness, safety, punctuality, friendliness, comfortability, user_id 
    FROM public.ratings 
    WHERE user_id = %s"""
    res = db.fetch_one(select_query, (user_id,))
    db.close_connection()
    print("get_ratings_for_logged_in_user()")
    if not res:
        raise Exception(f"No ratings found for user with id {user_id}.")
    return res
