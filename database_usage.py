# Import the Database class from your database.py file
from database import Database

# Initialize the database using the config file
db = Database(config_file='.\config.json')

# Connect to the database
db.connect()

# db.execute_query()  #use this when you dont expect any returns

# Fetch data
select_query = "SELECT * FROM custom_signal limit 5"
rows = db.fetch_all(select_query)
print(rows)

# Close the connection
db.close_connection()
