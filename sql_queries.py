import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= (""" create table if not exists staging_events
                                (
                                    artist varchar,
                                    auth varchar,
                                    firstName varchar,
                                    gender varchar,
                                    itemInSession int,
                                    lastName varchar,
                                    length numeric,
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration numeric,
                                    sessionId int,
                                    song varchar,
                                    status int,
                                    ts numeric,
                                    userAgent varchar,
                                    userId int
                                )""")

staging_songs_table_create = ("""create table if not exists staging_songs
                                (
                                    num_songs int,
                                    artist_id varchar,
                                    artist_latitude varchar,
                                    artist_longitude varchar,
                                    artist_location varchar,
                                    artist_name varchar,
                                    song_id varchar,
                                    title varchar,
                                    duration numeric,
                                    year int
                                )""")

songplay_table_create = ("""create table if not exists songplays
                            (
                                songplays_id bigint IDENTITY(0,1) primary key, 
                                start_time TIMESTAMP NOT NULL, 
                                user_id varchar NOT NULL, 
                                level varchar, 
                                song_id varchar, 
                                artist_id varchar, 
                                session_id int NOT NULL, 
                                location varchar, 
                                user_agent varchar
                            )""")

user_table_create = ("""create table if not exists users
                            (
                                user_id varchar PRIMARY KEY, 
                                first_name varchar NOT NULL, 
                                last_name varchar NOT NULL, 
                                gender varchar, 
                                level varchar
                            )""")

song_table_create = ("""create table if not exists songs
                            (
                                song_id varchar PRIMARY KEY, 
                                title varchar NOT NULL, 
                                artist_id varchar NOT NULL, 
                                year int, 
                                duration numeric
                            )""")

artist_table_create = ("""create table if not exists artists
                            (
                                artist_id varchar PRIMARY KEY, 
                                name varchar NOT NULL, 
                                location varchar, 
                                latitude numeric, 
                                longitude numeric
                            )""")

time_table_create = ("""create table if not exists times
                            (
                                start_time TIMESTAMP primary key, 
                                hour int, 
                                day int, 
                                week int, 
                                month int, 
                                year int, 
                                weekday varchar
                            )""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} 
                        credentials 'aws_iam_role={}'
                        json {}
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from {} 
                        credentials 'aws_iam_role={}'
                        json 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            select dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp), userId, level, s.song_id , ar.artist_id 
                            , sessionId, se.location, userAgent 
                            from staging_events se join artists ar on se.artist = ar.name join songs s on se.song = s.title where se.page='NextSong' """)

user_table_insert = ("""insert into users(user_id, first_name, last_name, gender, level) select DISTINCT userId, firstName, lastName, gender, level
                            from staging_events where userId is not null and page='NextSong'
""")

song_table_insert = ("""insert into songs(song_id, title, artist_id, year, duration) select song_id, title, artist_id, year, duration from staging_songs
""")

artist_table_insert = ("""insert into artists(artist_id, name, location, latitude, longitude) 
                            select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs
""")

time_table_insert = ("""insert into times(start_time, hour, day, week, month, year, weekday) 
                        select dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp) as start_time, 
                        EXTRACT(HOUR FROM dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp)) as hour,
                        EXTRACT(DAY FROM  dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp)) as day,
                        EXTRACT(WEEK FROM  dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp)) as week,
                        EXTRACT(MONTH FROM  dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp)) as month,
                        EXTRACT(YEAR FROM  dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp)) as year,
                        EXTRACT(dow  FROM  dateadd(sec, (ts/1000)::integer, '1970-01-01'::timestamp)) as weekday 
                        from staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
