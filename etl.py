import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, date_format
import zipfile 
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


######## This Function is used to config the spark session ########
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

######## This udf is used to get datetime, year, month, week, weekday, day, and hour from a ts ########
@udf (StructType([
    StructField('start_time', TimestampType()),
    StructField('year', IntegerType()),
    StructField('month', IntegerType()),
    StructField('week', IntegerType()),
    StructField('weekday', IntegerType()),
    StructField('day', IntegerType()),
    StructField('hour', IntegerType())]))
def get_timestamp(line):
    ts = datetime.fromtimestamp(line/1000.0)
    return {
        'start_time': ts,
        'year': ts.year,
        'month': ts.month,
        'week':ts.isocalendar()[1],
        'weekday': ts.weekday(),
        'day': ts.day,
        'hour':ts.hour
    }

######## This Function is used to process song data, and write data to song, and artist bucket ########
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    print ('get file name')
    # read song data file
    #df = spark.read.json(song_data)
    
    ###### test it using local small file  ######
    ###### comment it out ######
    df = spark.read.json("./data/song_data/*/*/*/*.json")
    
    print ('read song data file done')
    # create temp view for staging table 
    df.createOrReplaceTempView("staging_song")

    # extract columns to create songs table
    songs_table = spark.sql(" SELECT artist_id, song_id, duration, title, year \
                              FROM staging_song \
                              GROUP BY artist_id, song_id, duration, title, year")
    
    print ('songs table created')
    
    # write songs table to parquet files partitioned by year and artist to song bucket
    song_output = output_data + 'song/'
    songs_table.write.mode('append').partitionBy("artist_id", "year").parquet(song_output)
    
    print ('song table written to s3')

    # extract columns to create artists table
    artists_table = spark.sql("select artist_id, artist_latitude, artist_location, artist_longitude, artist_name \
                               from staging_song\
                               GROUP BY artist_id, artist_latitude, artist_location, artist_longitude, artist_name")
    
    print ('artist table created')
    
    # write artists table to parquet files to artist bucket 
    
    artist_output = output_data + 'artist/'
    artists_table.write.mode('append').parquet(artist_output)
    
    print ('artist table written to s3')

######## This Function is used to process log data, and write data to user, time and songplays bucket ########
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    # df = spark.read.json(log_data)
    ###### test it using local small file  ######
    ###### comment it out ######
    df = spark.read.json("./data/*.json")
    print ('log data read')
    
    # filter by actions for song plays
    df = df.filter(df.page =='NextSong')
    df.createOrReplaceTempView("staging_events")

    # extract columns for users table    
    users_table = spark.sql("select firstname, gender, lastname, level, userid \
                               from staging_events \
                              group by firstname, gender, lastname, level, userid")
    print ('user table created')
    
    # write users table to parquet files on user bucket
    user_output = output_data + 'user/'
    users_table.write.mode('append').parquet(user_output) 
    print ('user data uploaded')
    
    # create timestamp column from original timestamp column
    # create datetime column from original timestamp column
    df = df.withColumn('date',get_timestamp('ts'))
    fields = ["start_time", "year","month", "week", "weekday", "day", "hour"]
    exprs = [ "date['{}'] as {}".format(field,field) for field in fields]
    
    # extract columns to create time table
    time_table = df.selectExpr(*exprs)
    time_table.createOrReplaceTempView("time")
    print ('time table created ')
    
    # write time table to parquet files partitioned by year and month on time bucket
    time_output = output_data + 'time/'
    time_table.write.mode('append').partitionBy("year", "month").parquet(time_output)
    print ('time data uploaded ')
    
    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    # song_df = spark.read.json(log_data)
    ###### test it using local small file  ######
    ###### comment it out ######
    song_df = spark.read.json('./data/song_data/*/*/*/*.json')
    
    print ('read song data file done')
    song_df.createOrReplaceTempView("staging_song")

    # extract columns from joined song and log datasets to create songplays table 
    df_log = df.selectExpr(["date['start_time'] as start_time","userId","sessionId","level","artist","song","useragent"])
    df_log.createOrReplaceTempView("staging_events")
    
    # since we need to partition by year and month, I also join with time table to get the year and month 
    songplays_table = spark.sql(" select b.artist_id, level, artist_location, sessionid, song_id, a.start_time, \
                                  useragent, userId, t.month, t.year from staging_events a \
                                  join staging_song b on a.artist = b.artist_name \
                                  and a.song = b.title \
                                  join time t on t.start_time = a.start_time ")
    
    # write songplays table to parquet files partitioned by year and month on songplays bucket 
    songplays_output = output_data + 'songplays/'
    songplays_table.write.mode('append').partitionBy("year", "month").parquet(songplays_output)

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-project/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
