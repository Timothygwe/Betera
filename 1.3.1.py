from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Making Spark session
sp = SparkSession.builder.master("local").appName("BETS").getOrCreate()

# Loading data from csv files
dfb = sp.read.csv('data/bets.csv', header=True, inferSchema=True)
dfe = sp.read.csv('data/events.csv', header=True, inferSchema=True)

# Merging two dfs on event_id
resulted_df = dfb.join(dfe, dfb['event_id'] == dfe['event_id'], 'inner').drop(dfe['event_id'])

# filtering by condition
filtered_df = resulted_df.where(
    (col('create_time') >= "2022-03-14 12:00:00") &
    (col('event_stage') == "Prematch") &
    (col('sport') == "E-Sports") &
    (col('bet_size') >= 10) &
    (col('accepted_odd') >= 1.5) &
    (col('settlement_time') <= "2022-03-15 12:00:00") &
    (col('bet_type') != "System") &
    (~col("item_result").isin(["Return", "Cashout", "FreeBet"]))
)



# geting unique player_id
resulted_set = set(filtered_df.select('player_id').distinct().rdd.map(lambda row: row[0]).collect())

print(resulted_set)
