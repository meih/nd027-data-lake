import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
import datetime
import pandas
# import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description:
        This function createa a new Spark session
    
    Argments:
        None
    
    Returns:
        A Spark session created
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        This function processes log data and creates the following parquet files:
        - songs
        - artists
    
    Argments:
        spark: Spark session object
        input_data: Directory to reference data
        output_data: Directory to store parquet files
    
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()
    
    # extract columns to create songs table
    songs_table = df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(
        output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("lattitude"),
        col("artist_longitude").alias("start_time")
    ).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Description:
        This function processes log data and creates the following parquet files:
        - users
        - time
        - songplays
    
    Argments:
        spark: Spark session object
        input_data: Directory to reference data
        output_data: Directory to store parquet files
    
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    df.printSchema()
    
    # filter by actions for song plays
    df = df[df.page == "NextSong"]

    # extract columns for users table
    users_table = df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        "gender",
        "level"
    ).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time',((df.ts.cast('float')/1000).cast("timestamp")))
    
    # extract columns to create time table
    time_table = df.select(
        col("start_time").alias("start_time"),
        hour("start_time").alias('hour'),
        dayofmonth("start_time").alias('day'),
        weekofyear("start_time").alias('week'),
        month("start_time").alias('month'), 
        year("start_time").alias('year'), 
        date_format(col("start_time"), "E").alias("weekday")
    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(output_data + "time")

    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (song_df.title == df.song) & (song_df.duration == df.length))
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = songplays_table.select(
        'songplay_id',
        (col('ts').cast('float')/1000).cast("timestamp").alias('start_time'),
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent')
    ).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year',year('start_time')).withColumn(
        'month',month('start_time')).write.mode(
        'overwrite').partitionBy('year','month').parquet(
        output_data + 'songplays')


def main():
    """
    Description:
        The main function.
        This creates a new Spark session, and calls subsequent functions to process the Sparkify data.
    
    Argments:
        None
    
    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake-project-meiko/"   
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
