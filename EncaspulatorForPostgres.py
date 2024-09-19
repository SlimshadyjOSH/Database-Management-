import psycopg2  # PostgreSQL database adapter for Python
import pandas as pd  # Library for data manipulation and analysis

# Class for managing database operations related to the Movie database
class MovieDatabaseManager:
    def __init__(self, server, database, user, password):
        self.server = server  # Database server address
        self.database = database  # Database name
        self.user = user  # Username for the database
        self.password = password  # Password for the database
        self.conn = None  # Placeholder for the database connection object

    # Method to establish a connection to the PostgreSQL database
    def connect_to_database(self):
        try:
            self.conn = psycopg2.connect(
                host=self.server,  # Database server
                database=self.database,  # Database name
                user=self.user,  # Database username
                password=self.password  # Database password
            )
            print("Connected to database")
        except psycopg2.Error as e:  # Catch and print any database connection errors
            print(f"Error connecting to database: {e}")

    # Method to retrieve data from a specific table in the database
    def get_data_from_database(self, table_name):
        try:
            query = f"SELECT * FROM {table_name}"  # SQL query to select all rows from the table
            if self.conn is not None:  # Check if the connection is established
                df = pd.read_sql(query, self.conn)  # Load the query result into a pandas DataFrame
                return df
            else:
                print("No connection established.")  # If there's no connection, print a message
                return None
        except psycopg2.Error as e:  # Catch any query execution errors
            print(f"Error executing query: {e}")
            return None

    # Method to close the database connection
    def close_connection(self):
        if self.conn is not None:  # If there's an open connection, close it
            self.conn.close()
            print("Connection closed")
        else:
            print("No connection to close.")  # If no connection is open, print a message

# Initialize the database connection manager with the necessary details
db = Database('localhost', 'Movies', 'postgres', '*****')
db.connect_to_database()  # Establish connection to the database

# Retrieve data from the movies, creative, and revenue tables
movies_df = db.get_data_from_database('movies')
creative_df = db.get_data_from_database('creative')
revenue_df = db.get_data_from_database('revenue')

# Merge the retrieved DataFrames on 'movie_id'
merged_df = pd.merge(movies_df, creative_df, on='movie_id')
merged_df = pd.merge(merged_df, revenue_df, on='movie_id')

# Group by 'creative_type' and calculate the average international box office revenue
genre_revenue_df = merged_df.groupby('creative_type')['international_box_office'].mean().reset_index()

# Sort the grouped data by average international box office revenue in descending order
genre_revenue_df = genre_revenue_df.sort_values('international_box_office', ascending=False)

# Format the international box office revenue values to include commas and two decimal points
genre_revenue_df['international_box_office'] = genre_revenue_df['international_box_office'].apply(
    lambda x: '${:,.2f}'.format(x) if not np.isnan(x) else x
)

# Display the top 5 creative types with the highest average international box office revenue
print("Top 5 creative types with the highest average international box office revenue:")
print(genre_revenue_df.head(5))

# Close the database connection
db.close_connection()
