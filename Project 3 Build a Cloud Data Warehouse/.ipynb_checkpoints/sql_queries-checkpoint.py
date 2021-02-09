import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table if exists staging_events_table"
staging_songs_table_drop = "DROP table if exists staging_songs_table"
songplay_table_drop = "DROP table if exists songplay_table"
user_table_drop = "DROP table if exists user_table"
song_table_drop = "DROP table if exists song_table"
artist_table_drop = "DROP table if exists artist_table"
time_table_drop = "DROP table if exists time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
artist varchar, 
auth varchar, 
firstName varchar, 
gender varchar, 
ItemInSession int,
lastName varchar, 
length float, 
level varchar, 
location varchar, 
method varchar,
page varchar, 
registration varchar, 
sessionId int, 
song varchar, 
status int,
ts bigint, 
userAgent varchar, 
userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
song_id varchar PRIMARY KEY,
artist_id varchar, 
artist_latitude float,
artist_longitude float, 
artist_location varchar, 
artist_name text,
duration float, 
num_songs int, 
title varchar, 
year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(
songplay_id int IDENTITY(0,1), 
start_time timestamp  NOT NULL, 
user_id int NOT NULL, 
level varchar NOT NULL, 
song_id varchar, 
artist_id varchar, 
session_id varchar NOT NULL, 
location varchar, 
user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table(
user_id int sortkey PRIMARY KEY, 
first_name varchar, 
last_name varchar,
gender varchar,
level varchar  NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(
song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar distkey, 
year int, 
duration float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(
artist_id varchar PRIMARY KEY distkey,
name varchar,
location varchar,
latitude float,
longitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
start_time timestamp PRIMARY KEY, 
hour int NOT NULL, 
day int NOT NULL, 
week int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday int NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events_table
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs_table
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table(
start_time, 
user_id, 
level, 
song_id, 
artist_id,
session_id, 
location, 
user_agent
)
SELECT
TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
e.userId, 
e.level, 
s.song_id, 
s.artist_id,
e.sessionId, 
e.location, 
e.userAgent
FROM
staging_events_table e
INNER JOIN 
staging_songs_table s
ON
e.song = s.title
AND e.artist = s.artist_name
AND e.length = s.duration
""")

user_table_insert = ("""
INSERT INTO user_table(
user_id, 
first_name, 
last_name, 
gender, 
level
)
SELECT
DISTINCT userId, 
firstName, 
lastName, 
gender, 
level
FROM
staging_events_table
WHERE
userId is not null
AND level is not null
""")

song_table_insert = ("""
INSERT INTO song_table(
song_id, 
title, 
artist_id, 
year, 
duration
)
SELECT
DISTINCT song_id, 
title, 
artist_id, 
year, 
duration
FROM
staging_songs_table
WHERE
song_id is not null
""")

artist_table_insert = ("""
INSERT INTO artist_table(
artist_id, 
name, 
location, 
latitude, 
longitude
)
SELECT
DISTINCT artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude
FROM
staging_songs_table
WHERE
artist_id is not null
""")

time_table_insert = ("""
INSERT INTO time_table(
start_time, 
hour, 
day, 
week, 
month, 
year, 
weekday
)
SELECT
TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
DATE_PART(hour, start_time) as hour,
DATE_PART(day, start_time) as day,
DATE_PART(week, start_time) as week,
DATE_PART(month, start_time) as month,
DATE_PART(year, start_time) as year,
DATE_PART(weekday, start_time) as weekday
FROM
staging_events_table
WHERE
start_time is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
