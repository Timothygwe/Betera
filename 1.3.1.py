from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем Spark сессию
sp = SparkSession.builder.master("local").appName("BETS").getOrCreate()

# Загружаем данные из CSV файлов
dfb = sp.read.csv('data/bets.csv', header=True, inferSchema=True)
dfe = sp.read.csv('data/events.csv', header=True, inferSchema=True)

# Объединяем два DataFrame по столбцу 'event_id'
resulted_df = dfb.join(dfe, dfb['event_id'] == dfe['event_id'], 'inner').drop(dfe['event_id'])

# Фильтрация по условиям
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



# Получаем уникальные player_id
resulted_set = set(filtered_df.select('player_id').distinct().rdd.map(lambda row: row[0]).collect())

print(resulted_set)
