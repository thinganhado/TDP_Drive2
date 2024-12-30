from database import Database

# Create the database instance
db = Database(config_file='./config.json')

# Function to connect to the database
def connect_db():
    db.connect()

# Function to close the database connection
def close_db():
    db.close_connection()
