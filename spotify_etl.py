import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sys
from sqlalchemy import create_engine
import psycopg2 as ps

def spotify_etl():
### Pull from Spotify API/ create dataframes #########################################################

    CLIENT_ID = "b6ac0a0e681b40ee925cd0c2ebc22143"
    CLIENT_SECRET = "0f82aedc79584039a65cc7594b977509"
    REDIRECT_URI = 'http://127.0.0.1:9090'             
    SCOPE = "user-read-recently-played" 
    
    sp = spotipy.Spotify(auth_manager = SpotifyOAuth(client_id = CLIENT_ID,
                                                     client_secret = CLIENT_SECRET,
                                                     redirect_uri = REDIRECT_URI,
                                                     scope = SCOPE ))
    recently_played = sp.current_user_recently_played(limit=50)
### precaution in case the length of recently_played is 0
    if len(recently_played)== 0:
        sys.exit("No Results Recieved From Spotify")
        
### SONG data/dataframe   
    song_list = []
    for row in recently_played['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_time_played = row['played_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'date_time_played':song_time_played,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
    
### cleaning SONG dataframe
    df_song = pd.DataFrame.from_dict(song_list)
    #date_time_played is an object (similar to string). Changing to a timestamp
    df_song['date_time_played'] = pd.to_datetime(df_song['date_time_played'])
    df_song['date_time_played'] = df_song['date_time_played'].dt.tz_convert('US/Eastern')
    #Remove the timezone part from the date/time/timezone.
    df_song['date_time_played'] = df_song['date_time_played'].astype(str).str[:-7]
    df_song['date_time_played'] = pd.to_datetime(df_song['date_time_played'])
    #This will be half of the unique identifier
    df_song['UNIX_Time_Stamp'] = (df_song['date_time_played'] - pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')
    #UID: so I can have the same song multiple times in my database but not the same song played at the same time
    df_song['unique_identifier'] = df_song['song_id'] + "-" + df_song['UNIX_Time_Stamp'].astype(str)
    df_song = df_song[['unique_identifier','song_id','song_name','duration_ms','url','popularity','date_time_played','album_id','artist_id']]
    
### ALBUM data/dataframe     
    album_list = [] 
    for row in recently_played['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,
                         'album_name':album_name,
                         'release_date':album_release_date,
                         'total_tracks':album_total_tracks,
                         'url':album_url}
        album_list.append(album_element)
    #Drop duplicates because I don't want to load two albums into SQL and then have them dropped
    df_album = pd.DataFrame.from_dict(album_list)
    df_album = df_album.drop_duplicates(subset=['album_id'])
    
## ARTIST data/dataframe      
    artist_list = []
    for row in recently_played['items']:
        for key,value in row.items():
            if key == "track":
                for data_point in value['artists']:
                    artist_id = data_point['id']
                    artist_name = data_point['name']
                    artist_url = data_point['external_urls']['spotify']
                    artist_element = {'artist_id':artist_id,
                                      'artist_name':artist_name,
                                      'artist_url':artist_url}
                    artist_list.append(artist_element)
    #Drop duplicates once again 
    df_artist = pd.DataFrame.from_dict(artist_list)
    df_artist = df_artist.drop_duplicates(subset=['artist_id'])  
    
### Connect to Postrgres and Load Data ###############################################################

    conn = ps.connect(dbname='Spotify',
                     user = 'postgres',
                     password = '#Butterfly7',
                     host = 'localhost',
                     port = '5432')
    cur = conn.cursor()

    engine = create_engine('postgresql+psycopg2://postgres:#Butterfly7@localhost/Spotify')
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()

### TRACKS: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_track AS SELECT * FROM spotify_schema.spotify_track LIMIT 0
    """)
    df_song.to_sql("tmp_track", schema= 'spotify_schema', con = engine,  if_exists='append', index = False)

### Moving data from temp table to main table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_track
    SELECT spotify_schema.tmp_track.*
    FROM   spotify_schema.tmp_track
    LEFT   JOIN spotify_schema.spotify_track USING (unique_identifier)
    WHERE  spotify_schema.spotify_track.unique_identifier IS NULL;
    DROP TABLE spotify_schema.tmp_track""")
    conn.commit()

### ALBUM: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_album AS SELECT * FROM spotify_schema.spotify_album LIMIT 0
    """)
    df_album.to_sql("tmp_album", schema = 'spotify_schema', con = engine, if_exists='append', index = False)
    conn_eng.commit()

### Moving from temp table to main Table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_album
    SELECT spotify_schema.tmp_album.*
    FROM   spotify_schema.tmp_album
    LEFT   JOIN spotify_schema.spotify_album USING (album_id)
    WHERE  spotify_schema.spotify_album.album_id IS NULL;

    DROP TABLE spotify_schema.tmp_album""")
    conn.commit()

### ARTIST: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_artist AS SELECT * FROM spotify_schema.spotify_artists LIMIT 0
    """)
    df_artist.to_sql("tmp_artist", schema = 'spotify_schema', con = engine, if_exists='append', index = False)
    conn_eng.commit()

### Moving data from temp table to main table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_artists
    SELECT spotify_schema.tmp_artist.*
    FROM   spotify_schema.tmp_artist
    LEFT   JOIN spotify_schema.spotify_artists USING (artist_id)
    WHERE  spotify_schema.spotify_artists.artist_id IS NULL;

    DROP TABLE spotify_schema.tmp_artist""")
    conn.commit()
    
    return "ETL completed"

### Execute:
spotify_etl()