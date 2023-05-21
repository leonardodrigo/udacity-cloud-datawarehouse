import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS public.staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.staging_songs;"

songplay_table_drop = "DROP TABLE IF EXISTS public.songplays;"
user_table_drop = "DROP TABLE IF EXISTS public.users;"
song_table_drop = "DROP TABLE IF EXISTS public.songs;"
artist_table_drop = "DROP TABLE IF EXISTS public.artists;"
time_table_drop = "DROP TABLE IF EXISTS public.time;"

# CREATE STAGE TABLES
staging_events_table_create= ("""
    CREATE TABLE public.staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session SMALLINT,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        session_id SMALLINT,
        song VARCHAR,
        status SMALLINT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id SMALLINT
    );
""")

# CREATE SCHEMA TABLES

staging_songs_table_create = ("""
    CREATE TABLE public.staging_songs (
        num_songs SMALLINT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.songplays (
        songplay_id BIGINT NOT NULL IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    ) DISTKEY(songplay_id) SORTKEY(start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    ) DISTKEY(user_id) SORTKEY(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year SMALLINT,
        duration FLOAT
    ) DISTKEY(song_id) SORTKEY(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude VARCHAR,
        longitude VARCHAR
    ) DISTKEY(artist_id) SORTKEY(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
    ) DISTKEY(start_time) SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY public.staging_events FROM {}
    iam_role {}
    FORMAT AS JSON {}
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY public.staging_songs FROM {}
    iam_role {}
    FORMAT AS JSON {}
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['MODE']['AUTO'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO public.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
              DISTINCT(DATEADD(s, evt.ts/1000, '20000101')) AS start_time
            , evt.user_id
            , evt.level
            , sng.song_id
            , sng.artist_id
            , evt.session_id
            , evt.location
            , evt.user_agent
        FROM
            public.staging_events evt
        LEFT JOIN public.staging_songs sng
            ON evt.song = sng.title
            AND evt.artist = sng.artist_name
        WHERE
            evt.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO public.users (user_id, first_name, last_name, gender, level)
        SELECT 
              DISTINCT user_id
            , first_name
            , last_name
            , gender
            , level
        FROM
            public.staging_events
        WHERE
            user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO public.songs (song_id, title, artist_id, year, duration)
        SELECT
              DISTINCT song_id
            , title
            , artist_id
            , year
            , duration
        FROM
            public.staging_songs
""")

artist_table_insert = ("""
    INSERT INTO public.artists (artist_id, name, location, latitude, longitude)
        SELECT
              DISTINCT artist_id
            , artist_name AS name
            , artist_location AS location
            , artist_latitude AS latitude
            , artist_longitude AS longitude
        FROM
            public.staging_songs
""")

time_table_insert = ("""
    INSERT INTO public.time (start_time, hour, day, week, month, year, weekday)
        SELECT 
              DISTINCT(DATEADD(s, ts/1000, '20000101')) AS start_time
            , EXTRACT(hour from start_time) AS hour
            , EXTRACT(day from start_time) AS day
            , EXTRACT(week from start_time) AS week
            , EXTRACT(month from start_time) AS month
            , EXTRACT(year from start_time) AS year
            , EXTRACT(weekday from start_time) AS weekday
        FROM 
            public.staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
