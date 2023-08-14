import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist          VARCHAR,
        auth            VARCHAR, 
        first_name      VARCHAR,
        gender          VARCHAR,   
        item_in_session INTEGER,
        last_name       VARCHAR,
        length          FLOAT,
        level           VARCHAR, 
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        session_id      INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              BIGINT,
        user_agent      VARCHAR,
        user_id         INTEGER
    );
    """
)

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id            VARCHAR,
        num_songs          INTEGER,
        title              VARCHAR,
        artist_name        VARCHAR,
        artist_latitude    FLOAT,
        year               INTEGER,
        duration           FLOAT,
        artist_id          VARCHAR,
        artist_longitude   FLOAT,
        artist_location    VARCHAR
    );
""")

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id     INTEGER     IDENTITY(0,1) PRIMARY KEY,
        start_time      TIMESTAMP   NOT NULL SORTKEY,
        user_id         INTEGER     NOT NULL DISTKEY,
        level           VARCHAR     ,
        song_id         VARCHAR     NOT NULL,
        artist_id       VARCHAR     NOT NULL,
        session_id      INTEGER     ,
        location        VARCHAR     ,
        user_agent      VARCHAR        
    );
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     INTEGER        NOT NULL PRIMARY KEY,
        first_name  VARCHAR        NOT NULL,
        last_name   VARCHAR        NOT NULL SORTKEY,
        gender      VARCHAR        ,
        level       VARCHAR           
    )
    DISTSTYLE ALL;
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     VARCHAR        NOT NULL PRIMARY KEY,
        title       VARCHAR        NOT NULL,
        artist_id   VARCHAR        NOT NULL SORTKEY,
        year        INTEGER        NOT NULL,
        duration    FLOAT           
    )
    DISTSTYLE ALL;
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   VARCHAR    NOT NULL PRIMARY KEY,
        name        VARCHAR    NOT NULL SORTKEY,
        location    VARCHAR    ,
        latitude    FLOAT      ,
        longitude   FLOAT           
    )
    DISTSTYLE ALL;
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time
    (
        start_time	TIMESTAMP   NOT NULL PRIMARY KEY SORTKEY,
        hour	    INTEGER     ,
        day	        INTEGER     ,
        week	    INTEGER     ,
        month	    INTEGER     NOT NULL,
        year	    INTEGER     ,
        weekday	    VARCHAR        
    )
    DISTSTYLE ALL;
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events from {source_location}
    CREDENTIALS 'aws_iam_role={creds}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT as JSON {json_info};
    """).format(
    source_location=config['S3']['LOG_DATA'],
    creds=config.get("IAM_ROLE", "ARN"),
    json_info=config['S3']['LOG_JSONPATH']
)

staging_songs_copy = (
    """
    COPY staging_songs from {source_location}
    CREDENTIALS 'aws_iam_role={creds}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """).format(
    source_location=config['S3']['SONG_DATA'],
    creds=config.get("IAM_ROLE", "ARN"),
)

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second') AS start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss
        ON se.artist = ss.artist_name
            AND se.song = ss.title
        WHERE ts IS NOT NULL
            AND user_id IS NOT NULL
            AND song_id IS NOT NULL
            AND artist_id IS NOT NULL
            AND page = 'NextSong';
    """
)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT user_id,
            first_name,
            last_name,
            gender,
            level
        FROM staging_events
        WHERE user_id IS NOT NULL
            AND first_name IS NOT NULL
            AND last_name IS NOT NULL;
    """
)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
            AND title IS NOT NULL
            AND artist_id IS NOT NULL
            AND year IS NOT NULL;
    """
)


artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
            AND artist_name IS NOT NULL;
    """
)

time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
                EXTRACT(hour FROM start_time) AS hour,
                EXTRACT(day FROM start_time) AS day,
                EXTRACT(week FROM start_time) AS week,
                EXTRACT(month FROM start_time) AS month,
                EXTRACT(year FROM start_time) AS year,
                EXTRACT(dayofweek FROM start_time) AS weekday
        FROM staging_events
        WHERE ts IS NOT NULL;
    """
)

# VALIDATE TABLES
se_validate = """SELECT COUNT (*) FROM staging_events"""
ss_validate = """SELECT COUNT (*) FROM staging_songs"""
sp_validate = """SELECT COUNT (*) FROM songplays"""
us_validate = """SELECT COUNT (*) FROM users"""
so_validate = """SELECT COUNT (*) FROM songs"""
ar_validate = """SELECT COUNT (*) FROM artists"""
ti_validate = """SELECT COUNT (*) FROM time"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop

]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]

validate_tables_queries = [
    se_validate,
    ss_validate,
    sp_validate,
    us_validate,
    so_validate,
    ar_validate,
    ti_validate
]
