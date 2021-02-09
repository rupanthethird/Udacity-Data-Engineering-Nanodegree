import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *

#make sure to get rid of quotations in config gile.
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This function creates a spark session.
    
    Returns:
    spark (SparkSession) - spark session connected to AWS EMR cluster
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads json file for song_data to extract, transform, and save as parquet files to a filepath that has been provided as an arugment.
    
    INPUTS:
    * spark - the spark session to run this function on
    * input_data - the filepath to the song_data which is resided in S3 bucket
    * output_data - the filepath to the transformed data which gets stored in another S3 bucket
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = song_df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])\
                    .withColumnRenamed('userId', 'user_id') \
                    .withColumnRenamed('firstName', 'first_name') \
                    .withColumnRenamed('lastName', 'last_name') \
                    .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    This function extracts json file for log_data, transform, and save as parquet files to a filepath that has been provided as an arugment.
    For the purpose of joining tables across the files(song_data and log_data), it extracts the loaded songs tbale which is processed in the process_song_data function.
    
    INPUTS:
    * spark - the spark session to run this function on
    * input_data - the filepath to the log_data which is resided in S3 bucket
    * output_data - the filepath to the transformed data which gets stored in another S3 bucket
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == "NextSong")

    # extract columns for users table    
    users_table = log_df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp))
    
    # extract columns to create time table
    time_table = log_df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))\
    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = os.path.join(output_data, 'songs/')
    song_data = spark.read.parquet(song_df)

    # extract columns from joined song and log datasets to create songplays table 
    joined_table = log_df.join(song_data, log_df.song == song_data.title, how='inner')
    
    songplays_table = joined_table.select("start_time",col("userId").alias("user_id"),"level","song_id","artist_id",col("sessionId").alias("session_id"),"location",col("userAgent").alias("user_agent"))\
                                   .withColumn("songplay_id", monotonically_increasing_id())
    
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                                      .select("songplay_id", songplays_table.start_time,"user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    """
    This function defines spark, input_data, and output_data and execute the pre-defined functions for ETL pipelines.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify01/parquet/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
