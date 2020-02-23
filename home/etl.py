import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import date_format
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear

# more imports for converting ts to timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id

# more imports for counting records
import pandas as pd
import numpy as np


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('USER', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('USER', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """ Define the components of the Spark Session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Function to process Song and Artist data from the json files under
        the song-data folder and load it in parquet format on a public S3 
        bucket. """
    # get filepath to song data file
    song_data = input_data+"song-data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data+"songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude')

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """ Function to process User, Time and Song Plays data from the json 
        files under the log-data folder and load it in parquet format on 
        a public S3 bucket. """
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table
    users_table = df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            "gender", "level")

    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # define ts format
    tsFormat = "yyyy-MM-dd HH:MM:ss z"

    # convert ts to a timestamp format
    time_table = df.withColumn('ts', to_timestamp(date_format((df.ts/1000).cast(dataType=TimestampType()), tsFormat), tsFormat))

    # extract columns to create time table
    time_table = time_table.select(col("ts").alias("start_time"),
                                   hour(col("ts")).alias("hour"),
                                   dayofmonth(col("ts")).alias("day"),
                                   weekofyear(col("ts")).alias("week"),
                                   month(col("ts")).alias("month"),
                                   year(col("ts")).alias("year"))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_data = input_data+"song-data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_df.join(df, song_df.artist_name == df.artist).withColumn("songplay_id", monotonically_increasing_id()).withColumn('start_time', to_timestamp(date_format((col("ts") / 1000).cast(dataType=TimestampType()), tsFormat), tsFormat)).select("songplay_id",
           "start_time",                
           col("userId").alias("user_id"),
           "level",
           "song_id",
           "artist_id",
           col("sessionId").alias("session_id"),
           col("artist_location").alias("location"),
           "userAgent",
           month(col("start_time")).alias("month"),
           year(col("start_time")).alias("year"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    # The following bucket has been created as public,
    # with full S3 acess on my account.
    output_data = "s3a://project4-spark/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
