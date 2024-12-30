from database import Database
from fastapi import HTTPException
from db_manager import db  # Import db from db_manager.py

# Function to get user by ID using a parameterized query
def get_user_by_id(user_id: int):
    select_query = "SELECT * FROM public.users WHERE user_id = %s"
    
    # Use parameterized query to avoid SQL injection
    user = db.fetch_all(select_query, (user_id,))
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user

# Function to get user by email (if email exists in the table)
def get_user_by_email(email: str):
    select_query = "SELECT * FROM public.users WHERE email = %s"
    
    # Use parameterized query to avoid SQL injection
    user = db.fetch_all(select_query, (email,))
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user


def check_user_exists(user_id: int) -> bool:
    db.connect()
    select_query = "SELECT EXISTS(SELECT 1 FROM public.users WHERE user_id = %s)"

    # Use parameterized query to avoid SQL injection
    result = db.fetch_all(select_query, (user_id,))
    db.close_connection()
    return result[0][0] if result else False

def get_user_trip(user_id: int):
    db.connect()
    select_query = """
    SELECT home_latitude, home_longitude, work_latitude, work_longitude, departure_time
    FROM public.users
    WHERE user_id = %s
    """
    result = db.fetch_one(select_query, (user_id,))
    db.close_connection()
    return result
