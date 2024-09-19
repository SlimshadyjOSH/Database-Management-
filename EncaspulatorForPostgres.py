import psycopg2
import pandas as pd

class MovieDatabaseManager:
    def __init__(self, server, database, user, password):
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def connect_to_database(self):
        try:
            self.conn = psycopg2.connect(
                host=self.server,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print("Connected to database")
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")

    def get_data_from_database(self, table_name):
        try:
            query = f"SELECT * FROM {table_name}"
            if self.conn is not None:
                df = pd.read_sql(query, self.conn)
                return df
            else:
                print("No connection established.")
                return None
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")
            return None

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()
            print("Connection closed")
        else:
            print("No connection to close.")

db = Database('localhost', 'Movies', 'postgres', '*****')
db.connect_to_database()

movies_df = db.get_data_from_database('movies')
creative_df = db.get_data_from_database('creative')
revenue_df = db.get_data_from_database('revenue')

merged_df = pd.merge(movies_df, creative_df, on='movie_id')
merged_df = pd.merge(merged_df, revenue_df, on='movie_id')

genre_revenue_df = merged_df.groupby('creative_type')['international_box_office'].mean().reset_index()

genre_revenue_df = genre_revenue_df.sort_values('international_box_office', ascending=False)

genre_revenue_df['international_box_office'] = genre_revenue_df['international_box_office'].apply(
    lambda x: '${:,.2f}'.format(x) if not np.isnan(x) else x
)

print("Top 5 creative types with the highest average international box office revenue:")
print(genre_revenue_df.head(5))

db.close_connection()
