from fastapi import APIRouter, HTTPException
from models.social import  SocialSimRequest, SocialSimResponse, SocialRatingResponse
from process.socialalgo.supabase_client import get_matched_user_ids, get_ratings_for_users
from crud.social import get_rating_sim as db_get_rating_sim



router = APIRouter()

@router.post("/socail/calculate_similarity", response_model=SocialSimResponse)
async def calculate_similarity(request: SocialSimRequest):

    # Step 1: Get matched user IDs
    matched_user_ids = get_matched_user_ids(request.user_id)
    print(f"Matched user IDs: {matched_user_ids}")

    if not matched_user_ids:
        raise HTTPException(status_code=404, detail="No matched users found.")

    # Step 2: Get ratings for the matched users
    print("Pass this step 1")
    ratings_data = get_ratings_for_users(matched_user_ids)
    print(f"Ratings data: {ratings_data}")

    if not ratings_data:
        raise HTTPException(status_code=404, detail="No ratings found for matched users.")

    # Step 3: Logic for calculating similarity or returning matched data
    # Assuming you want to return the matched user IDs and their ratings.
    return SocialSimResponse(matched_user_ids=matched_user_ids, ratings=ratings_data)
    # return {"matched_user_ids": matched_user_ids, "ratings": ratings_data}

@router.post("/social/calculate_similarity", response_model=SocialRatingResponse)
def calculate_similarity(request: SocialSimRequest):
    response = db_get_rating_sim(request)
    return response
