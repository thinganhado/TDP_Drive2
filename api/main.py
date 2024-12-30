from fastapi import APIRouter
from api.routers import trajectories, social

api_router = APIRouter()
api_router.include_router(trajectories.router, tags=['trajectories'])
api_router.include_router(social.router, tags=['social'])