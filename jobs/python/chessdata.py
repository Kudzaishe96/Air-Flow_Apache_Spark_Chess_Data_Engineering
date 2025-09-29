
from pyspark.sql import SparkSession
import requests
import pandas as pd
import datetime
import pycountry
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName("Chess").getOrCreate()

dfs = {

}

headers = {
    "User-Agent": "Manyanya (contact: kudzaishemanyanya@gmail.com)"
}

url = "https://api.chess.com/pub/leaderboards"
kue_url= "https://api.chess.com/pub/player/manyanya/stats"

player_profile = "https://api.chess.com/pub/player/erik"
player_stats = "https://api.chess.com/pub/player/erik/stats"
player_games = "https://api.chess.com/pub/player/erik/games"
player_game_archives = "https://api.chess.com/pub/player/erik/games/archives"
player_game_archives_pgn ="https://api.chess.com/pub/player/erik/games/2014/01/pgn"
country ="https://api.chess.com/pub/country/DE"
country_players = "https://api.chess.com/pub/country/DE/players"

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()

    # Iterate through each leaderboard category
    for category_name, category_data in data.items():
        # print(f"\nLeaderboard Category: {category_name}")

        # Convert the list of players into a DataFrame
        dfs[category_name] = pd.DataFrame(category_data)
    print(dfs.keys())

else:
    raise Exception(f"Failed to fetch: {response.status_code}")

# ...existing code...

#... schemas
schema_profile = StructType([
    StructField("avatar", StringType(), True),
    StructField("player_id", LongType(), True),
    StructField("@id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("followers", LongType(), True),
    StructField("country", StringType(), True),
    StructField("location", StringType(), True),
    StructField("last_online", LongType(), True),
    StructField("joined", LongType(), True),
    StructField("status", StringType(), True),
    StructField("is_streamer", BooleanType(), True),
    StructField("verified", BooleanType(), True),
    StructField("league", StringType(), True),
    StructField("streaming_platforms", StringType(), True)
])

country_schema = StructType([
    StructField("iso_code", StringType(), True),
    StructField("country_name", StringType(), True)
])

response = requests.get(kue_url, headers=headers)

if response.status_code == 200:
    data = response.json()

else:
    raise Exception(f"Failed to fetch: {response.status_code}")

def convert_date(ts):
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

# Create DataFrames
daily_df = pd.DataFrame({
    "rating": [data["chess_daily"]["last"]["rating"]],
    "last_play_date": [convert_date(data["chess_daily"]["last"]["date"])],
    "best_rating": [data["chess_daily"]["best"]["rating"]],
    "best_play_date": [convert_date(data["chess_daily"]["best"]["date"])],
    "best_game": [data["chess_daily"]["best"]["game"]],
    "wins": [data["chess_daily"]["record"]["win"]],
    "draws": [data["chess_daily"]["record"]["draw"]],
    "losses": [data["chess_daily"]["record"]["loss"]],
    "Time/Move": [data["chess_daily"]["record"]["time_per_move"]],
    "Timeout %": [data["chess_daily"]["record"]["timeout_percent"]]
})

rapid_df = pd.DataFrame({
    "rating": [data["chess_rapid"]["last"]["rating"]],
    "last_play_date": [convert_date(data["chess_rapid"]["last"]["date"])],
    "best_rating": [data["chess_rapid"]["best"]["rating"]],
    "best_play_date": [convert_date(data["chess_rapid"]["best"]["date"])],
    "best_game": [data["chess_rapid"]["best"]["game"]],
    "wins": [data["chess_rapid"]["record"]["win"]],
    "draws": [data["chess_rapid"]["record"]["draw"]],
    "losses": [data["chess_rapid"]["record"]["loss"]]
    
})

blitz_df = pd.DataFrame({
    "rating": [data["chess_blitz"]["last"]["rating"]],
    "last_play_date": [convert_date(data["chess_blitz"]["last"]["date"])],
    "best_rating": [data["chess_blitz"]["best"]["rating"]],
    "best_play_date": [convert_date(data["chess_blitz"]["best"]["date"])],
    "best_game": [data["chess_blitz"]["best"]["game"]],
    "wins": [data["chess_blitz"]["record"]["win"]],
    "draws": [data["chess_blitz"]["record"]["draw"]],
    "losses": [data["chess_blitz"]["record"]["loss"]]
})

bullet_df = pd.DataFrame({
    "rating": [data["chess_bullet"]["last"]["rating"]],
    "last_play_date": [convert_date(data["chess_bullet"]["last"]["date"])],
    "wins": [data["chess_bullet"]["record"]["win"]],
    "draws": [data["chess_bullet"]["record"]["draw"]],
    "losses": [data["chess_bullet"]["record"]["loss"]]
})

tactics_df = pd.DataFrame({
    "Highest rating": [data["tactics"]["highest"]["rating"]],
    "Highest Date": [convert_date(data["tactics"]["highest"]["date"])],
    "Lowest rating": [data["tactics"]["lowest"]["rating"]],
    "Lowest Date": [convert_date(data["tactics"]["lowest"]["date"])]
})

puzzle_rush_df = pd.DataFrame(data["puzzle_rush"]["best"], index=[0])

country_data = [(country.alpha_2, country.name) for country in pycountry.countries]

def clean_dataframe(df):
    """
    Ensures all columns in the DataFrame have consistent types.
    Converts columns with mixed types to string.
    """
    for col in df.columns:
        types = df[col].apply(type).unique()
        if len(types) > 1:
            df[col] = df[col].astype(str)
    return df

def convert_to_spark(dfs_dict, category):
    """
    Convert a specific pandas DataFrame from the leaderboard dictionary
    into a Spark DataFrame.
    """
    if category in dfs_dict:
        pdf = dfs_dict[category]
        pdf = clean_dataframe(pdf)  # Clean before conversion
        return spark.createDataFrame(pdf)
    else:
        raise ValueError(f"Category '{category}' not found in dictionary. Available keys: {list(dfs_dict.keys())}")

# ...existing code...

# Example usage:
daily = convert_to_spark(dfs, "daily")
live_rapid=convert_to_spark(dfs,"live_rapid")
live_blitz = convert_to_spark(dfs,"live_blitz")
live_bullet = convert_to_spark(dfs,"live_bullet")
countries = spark.createDataFrame(country_data, schema=country_schema)
daily_df  = spark.createDataFrame(daily_df)
rapid_df  = spark.createDataFrame(rapid_df)
bullet_df  = spark.createDataFrame(bullet_df)
tactics_df  = spark.createDataFrame(tactics_df)
puzzle_rush_df  = spark.createDataFrame(puzzle_rush_df)
blitz_df =spark.createDataFrame(blitz_df)



#align with the created table
def database_colums(table):

    jdbc_url = (
    "jdbc:sqlserver://kudzie.database.windows.net:1433;"
    "database=chess;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=120;"
    "hostNameInCertificate=*.database.windows.net;"
    )

    properties = {
    "user": "*",
    "password": "*",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

   
    df = spark.read.jdbc(url=jdbc_url, table=f"cs.{table}", properties=properties)
   #returns the secound column name up until the end
    return df.columns[1:]

#kue data cleaning
def kue_data_cleaning(df,table_name):
    df = df.withColumn("create_date",date_format(current_date(),"dd-MM-yyyy"))\
           .withColumn("create_datetime",date_format(current_timestamp(),"dd-MM-yyyy HH:mm:ss"))
    
    df = df.withColumn("create_date", to_date("create_date", "dd-MM-yyyy")) \
           .withColumn("create_datetime", to_timestamp("create_datetime", "dd-MM-yyyy HH:mm:ss")) 
    
    df = df.withColumn("last_play_date",date_format("last_play_date","dd-MM-yyyy"))\
           .withColumn("best_play_date",date_format("best_play_date","dd-MM-yyyy HH:mm:ss"))
    
    df = df.withColumn("last_play_date", to_date("last_play_date", "dd-MM-yyyy")) \
           .withColumn("best_play_date", to_timestamp("best_play_date", "dd-MM-yyyy HH:mm:ss"))   
    df = df.select(*database_colums(table_name))

    return df

#chess base data cleaning

def rename_join_data(df,table_name):
    
    # Drop unwanted columns if they exist
    for col_name in ["trend_score", "trend_rank", "flair_code"]:
        if col_name in df.columns:
            df = df.drop(col_name)

    #insert country code colum
    df = df.withColumn("country_code", substring(col("country"), -2, 2))
    # add create date and timestamp
    df = df.withColumn("create_date",date_format(current_date(),"dd-MM-yyyy"))\
           .withColumn("create_datetime",date_format(current_timestamp(),"dd-MM-yyyy HH:mm:ss"))
    
    df = df.withColumn("create_date", to_date("create_date", "dd-MM-yyyy")) \
           .withColumn("create_datetime", to_timestamp("create_datetime", "dd-MM-yyyy HH:mm:ss"))

    #rename columns
    df= df.withColumnRenamed("@id","link_id").withColumnRenamed("url","link_url").withColumnRenamed("score","rating")\
        .withColumnRenamed("rank","rank_per_day")
    #join contries table
    df=df.join(countries,countries.iso_code  == df.country_code ,how="left")

    #select the right columns from the database
    df = df.select(*database_colums(table_name))


    return df


#insert data into the database
def write_data_kue_insert(df, table):
    jdbc_url = (
        "jdbc:sqlserver://kudzie.database.windows.net:1433;"
        "database=chess;"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "loginTimeout=120;"
        "hostNameInCertificate=*.database.windows.net;"
    )

    properties = {
        "user": "*",
        "password": "*",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Drop 'id' column if present to avoid conflict with auto-increment
    #if "id" in df.columns:
        #df = df.drop("id")
    df=kue_data_cleaning(df,f"{table}")
    # Insert all records regardless of key value
    if df.count() > 0:
        df.write.mode("append").jdbc(url=jdbc_url, table=f"cs.{table}", properties=properties)
        print(f"{df.count()} records inserted into table {table}")
    else:
        print(f"No records to insert into table {table}")

#insert chess data into the database

def write_data_force_insert(df, table):
    jdbc_url = (
        "jdbc:sqlserver://kudzie.database.windows.net:1433;"
        "database=chess;"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "loginTimeout=120;"
        "hostNameInCertificate=*.database.windows.net;"
    )

    properties = {
        "user": "*",
        "password": "*",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Drop 'id' column if present to avoid conflict with auto-increment
    #if "id" in df.columns:
        #df = df.drop("id")
    df=rename_join_data(df,f"{table}")
    # Insert all records regardless of key value
    if df.count() > 0:
        df.write.mode("append").jdbc(url=jdbc_url, table=f"cs.{table}", properties=properties)
        print(f"{df.count()} records inserted into table {table}")
    else:
        print(f"No records to insert into table {table}")

#write data
write_data_force_insert(live_blitz,"live_blitz")
write_data_force_insert(daily,"daily")
write_data_force_insert(live_rapid,"live_rapid")
write_data_force_insert(live_bullet,"live_bullet")
write_data_kue_insert(rapid_df,"kue_rapid")

spark.stop()
