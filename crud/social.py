from process.socialalgo.social_algo import calculate_social_similarity
from process.socialalgo import supabase_client
from models.social import SocialSimRequest, SocialRatingItem, SocialRatingResponse

from database import Database

def get_rating_sim(request: SocialSimRequest):

    ratings = calculate_social_similarity(request.user_id)
    ratings_response = [SocialRatingItem(user_id=rate[0], rating = rate[1] ) for rate in ratings]

    return SocialRatingResponse(user_id=request.user_id, ratings=ratings_response)

