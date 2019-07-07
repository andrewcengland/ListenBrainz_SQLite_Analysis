import sqlite3
import pandas as pd
import json
import os

#### CHANGE DIRECTORY HERE ####
os.chdir("C:/DIRECTORY")

def database_setup(conn):
    print('Setting up database..')
    
    with conn:
        # Enable foreign keys
        conn.execute('PRAGMA foreign_keys = ON')
        
        # User table
        conn.execute('''CREATE TABLE IF NOT EXISTS User(
                     user_name TEXT PRIMARY KEY);''')
        
        # Artist table
        conn.execute('''CREATE TABLE IF NOT EXISTS Artist(
                     artist_id TEXT PRIMARY KEY, 
                     artist TEXT);''')
        
        # Album table
        conn.execute('''CREATE TABLE IF NOT EXISTS Album(
                     album_id TEXT PRIMARY KEY, 
                     album TEXT,
                     artist_id TEXT,
                     FOREIGN KEY (artist_id) REFERENCES Artist(artist_id));''')
        
        # Track table
        conn.execute('''CREATE TABLE IF NOT EXISTS Track(
                     track_id TEXT PRIMARY KEY,
                     track TEXT,
                     album_id TEXT,
                     FOREIGN KEY (album_id) REFERENCES Album(album_id));''')
        
        # Listen table
        conn.execute('''CREATE TABLE IF NOT EXISTS Listen(
                     user_name TEXT,
                     listen_time INTEGER,
                     track_id TEXT,
                     PRIMARY KEY (user_name, listen_time),
                     FOREIGN KEY (user_name) REFERENCES User(user_name),
                     FOREIGN KEY (track_id) REFERENCES Track(track_id));''')
     

def load_data(conn):
    print('Loading data into database..')
    
    with open('dataset.txt') as raw_data:
        # Set up the data arrays to be transferred
        users = []
        artists = []
        albums = []
        tracks = []
        listens = []
        
        # Filling the arrays with data
        for line in raw_data:
            # Parse the json document
            parsed_json = json.loads(line)
            
            # Declare the data variables
            user_name = parsed_json['user_name']
            album_id = parsed_json['track_metadata']['additional_info']['release_msid']
            artist_id = parsed_json['track_metadata']['additional_info']['artist_msid']
            track_id = parsed_json['track_metadata']['additional_info']['recording_msid']
            artist = parsed_json['track_metadata']['artist_name']
            track = parsed_json['track_metadata']['track_name']
            album = parsed_json['track_metadata']['release_name']
            listen_time = parsed_json['listened_at']
            
            # Set up the data for each table as tuples for compatibility with sqlite3's "executemany" method
            user_input = (user_name,)
            artist_input = (artist_id, artist)
            album_input = (album_id, album, artist_id)
            track_input = (track_id, track, album_id)
            listen_input = (user_name, listen_time, track_id)
            
            # Append the tuples to the data arrays
            users.append(user_input)
            artists.append(artist_input)
            albums.append(album_input)
            tracks.append(track_input)
            listens.append(listen_input)
           
        # Insert the array data into the database
        c = conn.cursor()
        c.executemany('INSERT OR IGNORE INTO User(user_name) VALUES(?)', users)
        c.executemany('INSERT OR IGNORE INTO Artist(artist_id, artist) VALUES(?, ?)', artists)
        c.executemany('INSERT OR IGNORE INTO Album(album_id, album, artist_id) VALUES(?, ?, ?)', albums)
        c.executemany('INSERT OR IGNORE INTO Track(track_id, track, album_id) VALUES(?, ?, ?)', tracks)
        c.executemany('INSERT OR IGNORE INTO Listen(user_name, listen_time, track_id) VALUES(?, ?, ?)', listens)
        conn.commit()


def date_index(conn):
    with conn:
        # Improves performance on queries involving the listen_time column in the Listen table
        conn.execute('CREATE INDEX IF NOT EXISTS IX_Listen_listen_time ON Listen (listen_time ASC)')


def data_analysis(conn):
    print('Performing data analysis..')
    
    # 10 Most Active Users
    most_active_df = pd.read_sql_query('''SELECT user_name, Count(*) AS songs_played 
                                       FROM Listen
                                       GROUP BY user_name ORDER BY Count(*) DESC LIMIT 10''', conn)
    
    # Number of Users Active on 01-03-2019
    active_users_df = pd.read_sql_query('''SELECT COUNT(DISTINCT user_name) AS active_users_on_01_03_2019 
                                        FROM Listen
                                        WHERE strftime('%d - %m - %Y', datetime(listen_time, 'unixepoch')) = "01 - 03 - 2019"''', conn)
    
    # User's first song
    first_songs_df = pd.read_sql_query('''SELECT l.user_name, t.track, MIN(datetime(l.listen_time, 'unixepoch')) AS time_played
                                       FROM Listen l
                                       INNER JOIN Track t on t.track_id = l.track_id
                                       GROUP BY l.user_name''', conn)
    
    # Write data analysis results to CSVs
    most_active_df.to_csv('most_active_users.csv', header=True, encoding='utf-8-sig')
    active_users_df.to_csv('active_users_on_date.csv', header=True, encoding='utf-8-sig') 
    first_songs_df.to_csv('users_first_songs.csv', header=True, encoding='utf-8-sig')  
    

def main():
    # Connects to an in-file database in the current working directory, or creates one, if it doesn't exist:
    conn = sqlite3.connect('ListenBrainz.db') 
    
    # Run functions
    database_setup(conn)
    load_data(conn)
    date_index(conn)
    data_analysis(conn)
    
    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()














