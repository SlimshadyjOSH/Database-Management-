#General Aim of the Code
#The purpose of this code is to connect to a database
#containing movie-related data, retrieving data from specific tables,
#merge that data, and analyze it to find the average international box office revenue grouped by creative type. 
#Finally, it displays the top five creative types based on their average revenue.


import pyodbc                          #pydoc library used to connect to databases.
import pandas as pd                    #pandas data manipulation library, alias pd.

#Defining database class
class Database:                                  # 5-9 _init_ method used when creating object.
    def __init__(self, server, database):        # Three parameters, self, server and database.
        self.server = server                     # self references to the object being created.
        self.database = database                 # server and database parameter used to connect to database.
        self.conn = None                         # instance variables: server, database self.conn. Values are set as arguments. Self.conn set to None means n0 database connection.
        
#Defining connect method 12-17
    def connect(self):                           #Connect method establish a connection to database using pydoc
        try:
            self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.server+';DATABASE='+self.database+';Trusted_Connection=yes') #Connection string is constructed using the server and database instance variables, and the Trusted_Connection=yes parameter indicates that the connection should use Windows authentication.
            print("Connected to database")
        except pyodbc.Error as e:
            print(f"Error connecting to database: {e}")    #If the connection is successful, the connect method prints "Connected to database" to the console. If an error occurs, it catches the pyodbc.Error exception and prints an error message to the console.

#Defining get_data method 20-27
    def get_data(self, table_name):                        #get_data methodretrieves data from a database table. table_name parameter, which specifies the table from which to retrieve data.
        try:
            query = f"SELECT * FROM {table_name}"          #Constructs a SQL query using the table_name parameter and executes it using the pd.read_sql function, which returns a Pandas DataFrame object. If the query is successful, the method returns the DataFrame object.
            df = pd.read_sql(query, self.conn)
            return df
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")           #If error occurs, it catches the pyodbc.Error exception and prints an error message to the console, returning None to indicate that no data was retrieved.
            return None                        

#Defining close_connection method 30-35
    def close_connection(self):                            #close_connection method that closes the database connection. It attempts to close the connection using the self.conn.close() method, and prints "Connection closed" to the console if successful.
        try:
            self.conn.close()
            print("Connection closed")
        except pyodbc.Error as e:
            print(f"Error closing connection: {e}")        #If error occurs, it catches the pyodbc.Error exception and prints an error message to the console.

# Creating Database Object
db = Database('LAPTOP-DCCRK0JG', 'Moviesdb')               #Database object with the server name LAPTOP-DCCRK0JG and database name Moviesdb, and calls the connect method to establish a connection to the database.
db.connect()

# Get data from the Movies, Creative, and Revenue tables
movies_df = db.get_data('dbo.Movies')
creative_df = db.get_data('dbo.Creative')
revenue_df = db.get_data('dbo.Revenue')  

#Note: The resulting DataFrames are stored in movies_df, creative_df, and revenue_df, respectively.

# Merging data tables| movie_id primary key.
merged_df = pd.merge(movies_df, creative_df, on='movie_id')
merged_df = pd.merge(merged_df, revenue_df, on='movie_id')

# Grouping and Calculating Average Domestic Box Office Revenue
genre_revenue_df = merged_df.groupby('genre')['domestic_box_office'].mean().reset_index()

# Format the domestic_box_office column to display the $ symbol
genre_revenue_df['domestic_box_office'] = genre_revenue_df['domestic_box_office'].apply(lambda x: '${:,.2f}'.format(x))     #This line formats the domestic_box_office column to display the $ symbol and commas for thousands separators using the apply method and a lambda function.

# Sort the genres by average domestic box office revenue in descending order
genre_revenue_df = genre_revenue_df.sort_values('domestic_box_office', ascending=False)

# Print the top 5 genres with the highest average domestic box office revenue
print("Top 5 genres with the highest average domestic box office revenue:")
print(genre_revenue_df.head(5)) #head method counts the top 5 | line 59 ascending=false.

db.close_connection() # Always close the database connection for best practices.
