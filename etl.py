import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType
import pandas as pd

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ This function is created spark session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ This function is help to extract song and artists json files from S3 and 
        save them in a dataframe"""
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(col('song_id').alias('song_id'),
                            col('title').alias('title'),
                            col('artist_id').alias('artist_id'),
                            col('year').alias('year'),
                            col('duration').alias('duration')).dropDuplicates(subset = ['song_id'])
    print ('songs_table:',songs_table.count(),'rows')
    songs_table.printSchema()
    songs_table.show(1)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet('{}songs/'.format(output_data),'overwrite')
    
    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name','artist_location','artist_latitude',
                               'artist_longitude']).dropDuplicates(subset = ['artist_id'])
    artists_table.printSchema()
    artists_table.show(1)
    
    # write artists table to parquet files
    artists_table.write.parquet('{}artists/'.format(output_data),'overwrite')


def process_log_data(spark, input_data, output_data):
    """ This function set the log data Json files from S3 and extract user and time data. 
        And join log and song files."""
    
    # get filepath to log data file
    #log_data =os.path.join(input_data, "log_data/*/*/*.json")
    log_data =os.path.join(input_data, "log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table =  df.select(['userID', 'firstName', 'LastName', 'gender', 'level']).dropDuplicates(subset = ['userID'])
    users_table.printSchema()
    users_table.show(1)
    
    # write users table to parquet files
    users_table.write.parquet('{}users/'.format(output_data),'overwrite')

    # create timestamp column from original timestamp column
    #
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time',get_timestamp(df['ts'])) 
    
    # extract columns to create time table
    time_table = df.select(col('start_time').alias('start_time'),
                           hour('start_time').alias('hour'),
                           dayofmonth('start_time').alias('day'),
                           weekofyear('start_time').alias('week'), 
                           month('start_time').alias('month'),
                           year('start_time').alias('year'),
                           date_format('start_time','EEEE').alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet('{}time/'.format(output_data),'overwrite')
    
    time_table.printSchema()
    time_table.show(1)

    # read in song data to use for songplays table
    #song_df = spark.read.parquet('{}songs/*/*/*/*'.format(output_data))
    song_df = spark.read.parquet('{}songs/*/*/*'.format(output_data))
    artists_t = spark.read.parquet('{}artists/*'.format(output_data))
    
    df.createOrReplaceTempView("logs")
    song_df.createOrReplaceTempView("song")
    artists_t.createOrReplaceTempView("artist")
    time_table.createOrReplaceTempView('time')
    
    spark.sql("""
        SELECT * FROM song
    """).show(500)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(""" SELECT logs.ts as start_time, time.year, time.month,
                                        logs.userId, logs.level, 
                                        song.song_id, artist.artist_id, logs.sessionId, 
                                        artist.artist_location, logs.userAgent
                                    FROM logs
                                        JOIN song on
                                            logs.song = song.title AND
                                            logs.length=song.duration
                                        JOIN time on 
                                            logs.start_time = time.start_time
                                        JOIN artist on
                                            logs.artist = artist.artist_name
                                         """)
    
    # set songplay_id as serial
    songplays_table=songplays_table.withColumn('songplay_id',monotonically_increasing_id())
    print(songplays_table.limit(100).toPandas())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet('{}songplays/'.format(output_data),'overwrite')


def main():
    """ Main function calls spark session, input data and output data"""
    spark = create_spark_session()
    input_data = "data"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
