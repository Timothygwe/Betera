from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем Spark сессию
sp = SparkSession.builder.master("local").appName("BETS").getOrCreate()

# Загружаем данные из CSV файлов
dfb = sp.read.csv('data/bets.csv', header=True, inferSchema=True)
dfe = sp.read.csv('data/events.csv', header=True, inferSchema=True)

# Объединяем два DataFrame по столбцу 'event_id'
resulted_df = dfb.join(dfe, dfb['event_id'] == dfe['event_id'], 'inner').drop(dfe['event_id'])


filtered_df = (
    resulted_df
    .filter(col('create_time') >= "2022-03-14 12:00")
    .filter(col('event_stage') == "Prematch")
    .filter(col('sport') == "E-Sports")
    .filter(col('bet_size') >= 10)
    .filter(col('accepted_odd') >= 1.5)
    .filter(col('settlement_time') <= "2022-03-15 12:00")
    .filter(col('bet_type') != "System")
    .filter(~col("item_result").isin(["Return", "Cashout", "FreeBet"]))
)


resulted_set = set(filtered_df.select('player_id').distinct().rdd.flatMap( lambda x: x).collect())

print(resulted_set)

