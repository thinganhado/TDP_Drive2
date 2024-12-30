from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from database import Database
from contextlib import asynccontextmanager
from api.main import api_router
from crud.user import get_user_by_id, get_user_by_email 
from db_manager import connect_db, close_db  # Import database functions from db_manager
from process.address_validation import validate_address  # Import the validate_address function

# Create an instance of the Database class
db = Database(config_file='./config.json')

# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start up task: Connect to the database
    connect_db()
    yield
    # Shutdown task: Close the database connection
    close_db()

# FastAPI app with lifespan context manager
app = FastAPI(lifespan=lifespan)
app.include_router(api_router)

# Pydantic model to define the structure of incoming POST request for user by email
class GetUserByEmail(BaseModel):
    email: str

# Pydantic model for validation request
class ValidateAddressRequest(BaseModel):
    user_id: int

@app.get("/")
def root():
    return {"message": "Hello World"}

# Test database connection endpoint
@app.get("/test-db-connection")
def test_db_connection():
    try:
        # Test the connection by running a simple query
        db.execute_query("SELECT 1")
        return {"message": "Database connection successful"}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# Endpoint to get user by ID
@app.get("/user/{user_id}")
def get_user(user_id: int):
    try:
        user = get_user_by_id(user_id)
        print  # Call the imported function
        return {"user": user}
    except Exception as e:
        raise HTTPException(status_code=404, detail="User not found")

# Endpoint to get user by email (calls the function from crud/user.py)
@app.post("/user/email")
def get_user_by_email_endpoint(request: GetUserByEmail):
    try:
        user = get_user_by_email(request.email)  # Call the imported function
        return {"user": user}
    except Exception as e:
        raise HTTPException(status_code=404, detail="User not found")


# Endpoint to validate home and work addresses for a user
@app.post("/user/validate-address")
def validate_user_address(request: ValidateAddressRequest):
    try:
        # Call the validate_address function with the user_id
        validation_result = validate_address(request.user_id)
        
        if validation_result is None:
            raise HTTPException(status_code=404, detail="No valid home or work location found for validation")

        # If validation is successful
        return {"message": "Address validation successful"}
    
    except HTTPException as e:
        raise e  # Re-raise any HTTP exceptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during validation: {str(e)}")