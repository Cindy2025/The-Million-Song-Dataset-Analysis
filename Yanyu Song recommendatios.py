# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql import functions as F


# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Compute suitable number of partitions

conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M


# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------

mismatches_schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_artist", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_artist", StringType(), True),
    StructField("track_title", StringType(), True)
])

with open("/scratch-network/courses/2020/DATA420-20S2/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
    lines = f.readlines()
    sid_matches_manually_accepted = []
    for line in lines:
        if line.startswith("< ERROR: "):
            a = line[10:28]
            b = line[29:47]
            c, d = line[49:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
matches_manually_accepted.cache()
# matches_manually_accepted.show(10, 20)
# +------------------+-----------------+--------------------+------------------+--------------------+--------------------+
# |           song_id|      song_artist|          song_title|          track_id|        track_artist|         track_title|
# +------------------+-----------------+--------------------+------------------+--------------------+--------------------+
# |SOFQHZM12A8C142342|     Josipa Lisac|             razloga|TRMWMFG128F92FFEF2|        Lisac Josipa|        1000 razloga|
# |SODQSLR12A8C133A01|    John Williams|Concerto No. 1 fo...|TRWHMXN128F426E03C|English Chamber O...|II. Andantino sic...|
# +------------------+-----------------+--------------------+------------------+--------------------+--------------------+


# print(matches_manually_accepted.count())  # 488

with open("/scratch-network/courses/2020/DATA420-20S2/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
    lines = f.readlines()
    sid_mismatches = []
    for line in lines:
        if line.startswith("ERROR: "):
            a = line[8:26]
            b = line[27:45]
            c, d = line[47:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
mismatches.cache()
# mismatches.show(10, 20)
# +------------------+-------------------+--------------------+------------------+--------------+--------------------+
# |           song_id|        song_artist|          song_title|          track_id|  track_artist|         track_title|
# +------------------+-------------------+--------------------+------------------+--------------+--------------------+
# |SOUMNSI12AB0182807|Digital Underground|    The Way We Swing|TRMMGKQ128F9325E10|      Linkwood|Whats up with the...|
# |SOCMRBE12AB018C546|         Jimmy Reed|The Sun Is Shinin...|TRMMREB12903CEB1B1|    Slim Harpo|I Got Love If You...|
# |SOIYAAQ12A6D4F954A|           Excepter|                  OG|TRMWHRI128F147EA8E|    The Fevers|Não Tenho Nada (N...|
# +------------------+-------------------+--------------------+------------------+--------------+--------------------+


# print(mismatches.count())  # 19094

triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(triplets_schema)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
    .cache()
)
# triplets.cache()
# triplets.show(10, 50)
# +----------------------------------------+------------------+-----+
# |                                 user_id|           song_id|plays|
# +----------------------------------------+------------------+-----+
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQEFDN12AB017C52B|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOIUJ12A6701DAA7|    2|
# +----------------------------------------+------------------+-----+

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
# mismatches_not_accepted.show (10,50)
# +------------------+----------------------+-------------------------+------------------+--------------------+-----------------------+
# |           song_id|           song_artist|               song_title|          track_id|        track_artist|            track_title|
# +------------------+----------------------+-------------------------+------------------+--------------------+-----------------------+
# |SOAABVA12AC3DF673F|                      |                         |TRXVIGQ12903CE7F6E|        Vivian Girls|     Can't Get Over You|
# |SOAADIT12D021B4BDE|                      |                         |TRNUBFI128F42AB00B|            Westlife|              Evergreen|
# +------------------+----------------------+-------------------------+------------------+--------------------+-----------------------+

triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

# triplets_not_mismatched.show(10,50)
# +------------------+----------------------------------------+-----+
# |           song_id|                                 user_id|plays|
# +------------------+----------------------------------------+-----+
# |SOAAADE12A6D4F80CC|1c21e65e0e67ecc08f2da4856decda7e6cf6de2e|    1|
# |SOAAADE12A6D4F80CC|ae6b5e9dbfdd799f23fb1a5887de42d02338e2e4|    1|
# +------------------+----------------------------------------+-----+

####################################################################################### Q1

triplets_not_mismatched = triplets_not_mismatched.repartition(partitions).cache()

# print(mismatches_not_accepted.count())  # 19093
# print(triplets.count())                 # 48373586 triplets
# print(triplets_not_mismatched.count())  # 45795111



# triplets.select('song_id').distinct().count() 
# 384546

################### Q1(a) How many unique songs are there in the dataset? 

# triplets_not_mismatched.select('song_id').distinct().count()  triplets_not_mismatched
# 378310
# 
################### Q1(a) How many unique users?

# triplets.select('user_id').distinct().count() 
# 1019318

# triplets_not_mismatched.select('user_id').distinct().count() 
# 1019318

#  Q1(b) How many different songs has the most active user played?
# What is this as a percentage of the total number of unique songs in the dataset?

triplets_not_mismatched.select('user_id')\
    .groupBy('user_id')\
    .count()\
    .orderBy(col('count').desc())\
    .show(2, False)
# +----------------------------------------+-----+
# |user_id                                 |count|
# +----------------------------------------+-----+
# |ec6dfcf19485cb011e0b22637075037aae34cf26|4316 |
# |8cb51abc6bf8ea29341cb070fe1e1af5e4c3ffcc|1562 |
# +----------------------------------------+-----+

 # 4316 / 378310
 # 0.0114
 
###################### Q1(c) Visualize the distribution of song popularity and the distribution of user activity.
###################### What is the shape of these distributions?

#song-plays for popularity

song_unique_users =triplets_not_mismatched.groupBy("song_id").agg(F.count('user_id').alias('user_count'))
song_unique_users.sort('user_count', ascending=False).show(10,False)
# song_unique_users.coalesce(1).write.csv('song_unique_users',mode='overwrite',header = True)
#  triplets_not_mismatched.coalesce(1).write.csv('triplets_not_mismatched',mode='overwrite',header = True)
# triplets_not_mismatched.repartition(1).write.csv("hdfs:///user/yzh352/outputs/triplets_not_mismatched", header=Ture)
 # hdfs dfs -copyToLocal /user/yzh352/triplets_not_mismatched

# song_unique_users.show()
# +------------------+----------+
# |song_id           |user_count|
# +------------------+----------+
# |SOAXGDH12A8C13F8A1|90444     |
# |SOBONKR12A58A7A7E0|84000     |
# |SOSXLTC12AF72A7F54|80656     |
# |SONYKOW12AB01849C9|78353     |
# |SOEGIYH12A6D4FC0E3|69487     |
# |SOLFXKT12AB017E3E0|64229     |
# |SOFLJQZ12A6D4FADA6|58610     |
# |SOUSMXX12AB0185C24|53260     |
# |SOWCKVR12A8C142411|52080     |
# |SOUVTSM12AC468F6A7|51022     |
# +------------------+----------+


# song_unique_users.repartition(1).write.format('com.databricks.spark.csv').save("/users/home/yzh352/Distribution")
# song_unique_users.write.format("csv").save("hdfs:///user/yzh352/outputs/song.csv")
# song_unique_users.repartition(1).write.csv("hdfs:///user/yzh352/outputs/msd/song_unique_users", header=Ture)
#user-plays for user activity


user_unique_song =triplets_not_mismatched.groupBy("user_id").agg(F.count('song_id').alias('song_count'))
user_unique_song.sort('song_count', ascending=False).show(10,False)
#write user_unique_song
# user_unique_song.coalesce(1).write.csv('user_unique_song',mode='overwrite',header = True)
# hdfs dfs -copyToLocal /user/yzh352/user_unique_song user_unique_song
#################################################################################
# Qgenre_distribution = genre_not_mismatched.groupBy('genre').agg(count('track_id'))
# genre_distribution.show()

# from pyspark.mllib.stat import Statistics
# import pandas as pd
# import matplotlib.pyplot as plt

bin_left_locations, bin_counts =  song_unique_users.rdd.map(lambda x: x['user_count']).histogram(10)
# bin_left_locations, bin_counts =  song_unique_users.rdd.map(lambda x: x['user_count']) 
    
# bin_left_locations = np.array(bin_left_locations) 
# bin_centres = bin_left_locations + (bin_left_locations[1] - bin_left_locations[0]) / 2
# bin_centres = bin_centres[:-1]
# bin_counts = np.array(bin_counts)

plot the distribution
# output_path = os.path.expanduser("~/plots")
# fig, ax = plt.subplots(figsize=(15, 8))
# a = plt.bar(bin_left_locations, bin_counts)
# plt.title('Distribution of user activity')
# plt.xlabel('song_id')
# plt.ylabel('user_count')
# plt.xticks(rotation = 90) 
# fig.savefig(os.path.join('/users/home/yzh352/song Distribution.png'), bbox_inches="tight")
 
# -----------------------------------------------------------------------------
# Limiting
# -----------------------------------------------------------------------------
############################ Q1(d)Create a clean dataset of user-song plays by removing songs which have been played less
# than N times and users who have listened to fewer than M songs in total.
# -----------------------------------------------------------------------------
################################################################################# Data analysis
# -----------------------------------------------------------------------------


def get_user_counts(triplets):
    return (
        triplets
        .groupBy("user_id")
        .agg(
            F.count(col("song_id")).alias("song_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )

def get_song_counts(triplets):
    return (
        triplets
        .groupBy("song_id")
        .agg(
            F.count(col("user_id")).alias("user_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )

# User statistics

user_counts = (
    triplets_not_mismatched
    .groupBy("user_id")
    .agg(
        F.count(col("song_id")).alias("song_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
user_counts.cache()
# user_counts.count()

# 1019318

# user_counts.show(10, False)

# +----------------------------------------+----------+----------+
# |user_id                                 |song_count|play_count|
# +----------------------------------------+----------+----------+
# |093cb74eb3c517c5179ae24caf0ebec51b24d2a2|195       |13074     |
# |119b7c88d58d0c6eb051365c103da5caf817bea6|1362      |9104      |
# +----------------------------------------+----------+----------+

statistics = (
    user_counts
    .select("song_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
# print(statistics)

#               count                mean              stddev min    max
# song_count  1019318   44.92720721109605   54.91113199747355   3   4316
# play_count  1019318  128.82423149596102  175.43956510304616   3  13074

# user_counts.approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)# 75% user plays song more than 20 
# user_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)# 75% user olay more than 35 songs 

# [3.0, 20.0, 32.0,  58.0,  4316.0]
# [3.0, 35.0, 71.0, 173.0, 13074.0]

# Song statistics

song_counts = (
    triplets_not_mismatched
    .groupBy("song_id")
    .agg(
        F.count(col("user_id")).alias("user_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
song_counts.cache()
# song_counts.count()

# 378310

# song_counts.show(10, False)
# +------------------+----------+----------+
# |song_id           |user_count|play_count|
# +------------------+----------+----------+
# |SOBONKR12A58A7A7E0|84000     |726885    |
# |SOSXLTC12AF72A7F54|80656     |527893    |
# +------------------+----------+----------+


statistics = (
    song_counts
    .select("user_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
# print(statistics)

#              count                mean             stddev min     max
# user_count  378310  121.05181200602681  748.6489783736941   1   90444 
# play_count  378310   347.1038513388491  2978.605348838212   1  726885

song_counts.approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05) # 75% songs are played by 5 users 
song_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05) # 75% song is played 11 times

# [1.0,  5.0, 13.0, 104.0,  90444.0]
# [1.0, 11.0, 36.0, 217.0, 726885.0]











user_song_count_threshold = 20
song_user_count_threshold = 5

triplets_limited = triplets_not_mismatched

triplets_limited = (
    triplets_limited
    .join(
        triplets_limited.groupBy("user_id").count().where(col("count") > user_song_count_threshold).select("user_id"),# keep users at least listen 20 songs
        on="user_id",
        how="inner"
    )
)

triplets_limited = (
    triplets_limited
    .join(
        triplets_limited.groupBy("song_id").count().where(col("count") > song_user_count_threshold ).select("song_id"),# keep songs more than 11 times
        on="song_id",
        how="inner"
    )
)

triplets_limited.cache()
# triplets_limited.count()
# 34020840
(
    triplets_limited
    .agg(
        countDistinct(col("user_id")).alias('user_count'),
        countDistinct(col("song_id")).alias('song_count')
    )
    .toPandas()
    .T
    .rename(columns={0: "value"})
)


             # value
# user_count  393886
# song_count  242425



def get_user_counts(triplets):
    return (
        triplets
        .groupBy("user_id")
        .agg(
            F.count(col("song_id")).alias("song_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )

def get_song_counts(triplets):
    return (
        triplets
        .groupBy("song_id")
        .agg(
            F.count(col("user_id")).alias("user_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )
 

# -----------------------------------------------------------------------------
# Encoding
# -----------------------------------------------------------------------------

# Imports

from pyspark.ml.feature import StringIndexer

# Encoding

user_id_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_encoded")
song_id_indexer = StringIndexer(inputCol="song_id", outputCol="song_id_encoded")

user_id_indexer_model = user_id_indexer.fit(triplets_limited)
song_id_indexer_model = song_id_indexer.fit(triplets_limited)

triplets_limited = user_id_indexer_model.transform(triplets_limited)
triplets_limited = song_id_indexer_model.transform(triplets_limited)

# -----------------------------------------------------------------------------
# Splitting
# -----------------------------------------------------------------------------
################################ Q1(e) Split the user-song plays into training and test sets. Make sure that the test set contains at
################################       least 20% of the plays in total.
# Imports

from pyspark.sql.window import *

# Splits

training, test = triplets_limited.randomSplit([0.7, 0.3])

test_not_training = test.join(training, on="user_id", how="left_anti")

training.cache()
test.cache()
test_not_training.cache()


# print(f"training:          {training.count()}")
# print(f"test:              {test.count()}")
# print(f"test_not_training: {test_not_training.count()}")


# training:          27526331
# test:              11789045
# test_not_training: 4


counts = test_not_training.groupBy("user_id").count().toPandas().set_index("user_id")["count"].to_dict()
temp = (
    test_not_training
    .withColumn("id", monotonically_increasing_id())
    .withColumn("random", rand())
    .withColumn(
        "row",
        row_number()
        .over(
            Window
            .partitionBy("user_id")
            .orderBy("random")
        )
    )
)

for k, v in counts.items():
    temp = temp.where((col("user_id") != k) | (col("row") < v * 0.7))

temp = temp.drop("id", "random", "row")
temp.cache()

# temp.show(50, False)
training = training.union(temp.select(training.columns))
test = test.join(temp, on=["user_id", "song_id"], how="left_anti")
test_not_training = test.join(training, on="user_id", how="left_anti")

# print(f"training:          {training.count()}")
# print(f"test:              {test.count()}")
# print(f"test_not_training: {test_not_training.count()}")

# training:          27520263
# test:              11795113
# test_not_training: 0

# +------------------------------------------------------------------------------+
# Q2
# +------------------------------------------------------------------------------+

################################################ Q2(a)ALS


# Imports

from pyspark.ml.recommendation import ALS

from pyspark.mllib.evaluation import RankingMetrics
from pyspark.ml.evaluation import RegressionEvaluator

# Modeling

als = ALS(maxIter=5, regParam=0.01, userCol="user_id_encoded", itemCol="song_id_encoded", ratingCol="plays", implicitPrefs=True)
als_model = als.fit(training)
predictions = als_model.transform(test)

predictions = predictions.orderBy(col("user_id"), col("song_id"), col("prediction").desc())
predictions.cache()

predictions.show(5, False)
# +----------------------------------------+------------------+-----+---------------+---------------+-------------+
# |user_id                                 |song_id           |plays|user_id_encoded|song_id_encoded|prediction   |
# +----------------------------------------+------------------+-----+---------------+---------------+-------------+
# |0000267bde1b3a70ea75cf2b2d216cb828e3202b|SOCAFDI12A8C13D10E|1    |547869.0       |340.0          |0.07021535   |
# |0000267bde1b3a70ea75cf2b2d216cb828e3202b|SOCJCVE12A8C13CDDB|1    |547869.0       |376.0          |0.071913294  |
# |0000267bde1b3a70ea75cf2b2d216cb828e3202b|SODEYDM12A58A77072|1    |547869.0       |221.0          |0.07513414   |
# |0000267bde1b3a70ea75cf2b2d216cb828e3202b|SOPOFBW12AB0187196|1    |547869.0       |1773.0         |0.027420605  |
# |0000267bde1b3a70ea75cf2b2d216cb828e3202b|SOPQGWI12A8C135DDB|1    |547869.0       |2830.0         |0.027352666  |
# +----------------------------------------+------------------+-----+---------------+---------------+-------------+

#calculate rmse
evaluator = RegressionEvaluator(metricName="rmse", labelCol="plays", predictionCol="prediction")
evaluator.evaluate(predictions.filter(F.col('prediction') != np.NaN))    # 6.604064433013884
################################################################################################# Q2(b)
# Generate top 5 movie recommendations for a specified set of users
#mannually create a subset from test, considering different plays or rating may affect
# the result,I choose three users with different level 
test.show()
# +--------------------+------------------+-----+---------------+---------------+
# |             user_id|           song_id|plays|user_id_encoded|song_id_encoded|
# +--------------------+------------------+-----+---------------+---------------+
# |00007ed2509128dcd...|SOWCISX12AB0182F21|    1|       225819.0|        79013.0|
# |0000bb531aaa657c9...|SODHLYW12A8C135517|    1|        54380.0|        25169.0|
# +--------------------+------------------+-----+---------------+---------------+
only showing top 20 rows


test_user = test.where(F.col("user_id_encoded").isin({266303, 0, 133106})).select('user_id_encoded').distinct()
test_user.show()
# +---------------+
# |user_id_encoded|
# +---------------+
# |       266303.0|
# |       133106.0|
# |            0.0|
# +---------------+
test_user = test.where(F.col("user_id_encoded").isin({266303, 0, 133106}))

test_user.show()

# +--------------------+------------------+-----+---------------+---------------+
# |             user_id|           song_id|plays|user_id_encoded|song_id_encoded|
# +--------------------+------------------+-----+---------------+---------------+
# |0996658d5a8ce48a0...|SOBFRNE12A6D4F7995|    5|       266303.0|          115.0|
# |50d22c219a49fe8a4...|SOIKQFR12A6310F2A6|   15|       133106.0|         1398.0|
# +--------------------+------------------+-----+---------------+---------------+


#recommendations for subset
user_recs=alsModel.recommendForUserSubset(test_user, 5)
user_recs=als_model.recommendForUserSubset(test_user, 5)
 
user_recs.show(3,False)

 
# +---------------+-------------------------------------------------------------------------------------------+
# |user_id_encoded|recommendations                                                                            |
# +---------------+-------------------------------------------------------------------------------------------+
# |266303         |[[29, 0.7723557], [2, 0.7722361], [1, 0.75830567], [11, 0.74908656], [3, 0.7406801]]       |
# |133106         |[[0, 0.6175954], [37, 0.5202909], [47, 0.5092783], [97, 0.46350035], [112, 0.44147813]]    |
# |0              |[[180, 0.11685072], [36, 0.11274357], [48, 0.11079071], [31, 0.1021741], [19, 0.101758905]]|
# +---------------+-------------------------------------------------------------------------------------------+
#checking the user 266303 have listened to those songs or not
 test.show()
# +--------------------+------------------+-----+---------------+---------------+
# |             user_id|           song_id|plays|user_id_encoded|song_id_encoded|
# +--------------------+------------------+-----+---------------+---------------+
# |00007ed2509128dcd...|SOWCISX12AB0182F21|    1|       225819.0|        79013.0|
# |0000bb531aaa657c9...|SODHLYW12A8C135517|    1|        54380.0|        25169.0|
# +--------------------+------------------+-----+---------------+---------------+

user1 = test.filter(F.col('user_id_encoded') == 266303)
user1.show()
# +--------------------+------------------+-----+---------------+---------------+
# |             user_id|           song_id|plays|user_id_encoded|song_id_encoded|
# +--------------------+------------------+-----+---------------+---------------+
# |0996658d5a8ce48a0...|SOBFRNE12A6D4F7995|    5|       266303.0|          115.0|
# |0996658d5a8ce48a0...|SONDPRQ12A6D4F50E0|    5|       266303.0|         5715.0|
# +--------------------+------------------+-----+---------------+---------------+
#checking the user 266303 have listened to those songs or not
user1.where(F.col('song_id_encoded') == 29 ).count()  #0
user1.where(F.col('song_id_encoded') ==  2 ).count()  #0
user1.where(F.col('song_id_encoded') == 1 ).count()  #0
user1.where(F.col('song_id_encoded') == 11 ).count()  #0
user1.where(F.col('song_id_encoded') == 3).count()  #0

#checking the user 133106 have listened to those songs or not
user2 = test.filter(F.col('user_id_encoded') == 133106)
user2.where(F.col('song_id_encoded') == 0 ).count()  #0
user2.where(F.col('song_id_encoded') ==  37 ).count()  #0
user2.where(F.col('song_id_encoded') == 47 ).count()  #0
user2.where(F.col('song_id_encoded') == 97 ).count()  #0
user2.where(F.col('song_id_encoded') == 112).count() # 0

# the recommended songs have not been played by this user


#checking the user 0 have listened to those songs or not
 
user3 = test.filter(F.col('user_id_encoded') == 0)
user3.where(F.col('song_id_encoded') == 180 ).count()  #0
user3.where(F.col('song_id_encoded') ==  36 ).count()  #0
user3.where(F.col('song_id_encoded') == 48 ).count()  #0
user3.where(F.col('song_id_encoded') == 31 ).count()  #0
user3.where(F.col('song_id_encoded') == 19).count()   #1
# one of the five recommended songs have been played by this user

######################################################################Q2(c)Calculate Precision MAP and NDCG
from pyspark.sql.window import Window
import numpy as np 
predictions1 = predictions.filter(F.col('prediction') != np.NaN)
predictions1.show()
# +--------------------+------------------+-----+---------------+---------------+-----------+
# |             user_id|           song_id|plays|user_id_encoded|song_id_encoded| prediction|
# +--------------------+------------------+-----+---------------+---------------+-----------+
# |0000267bde1b3a70e...|SOCAFDI12A8C13D10E|    1|       547869.0|          340.0| 0.07021535|
# |0000267bde1b3a70e...|SOCJCVE12A8C13CDDB|    1|       547869.0|          376.0|0.071913294|
# +--------------------+------------------+-----+---------------+---------------+-----------+

#-------
k=5
#-------

# songs predicted top rank k
windowSpec = Window.partitionBy('user_id_encoded').orderBy(F.col('prediction').desc())
perUserPredictedItemsDF = predictions1 \
    .select('user_id_encoded', 'song_id_encoded', 'prediction', F.rank().over(windowSpec).alias('rank')) \
    .where('rank <= {0}'.format(k)) \
    .groupBy('user_id_encoded') \
    .agg(expr('collect_list(song_id_encoded) as songs_rec'))
perUserPredictedItemsDF.show(10,False)

 
# +---------------+----------------------------------+
# |user_id_encoded|songs_rec                         |
# +---------------+----------------------------------+
# |6.0            |[221.0, 289.0, 94.0, 366.0, 40.0] |
# |12.0           |[60.0, 290.0, 64.0, 361.0, 462.0] |
# |13.0           |[15.0, 39.0, 200.0, 57.0, 279.0]  |
# |58.0           |[152.0, 724.0, 18.0, 135.0, 291.0]|
# |79.0           |[46.0, 41.0, 60.0, 215.0, 47.0]   |
# |87.0           |[193.0, 48.0, 148.0, 707.0, 717.0]|
# |93.0           |[93.0, 164.0, 427.0, 698.0, 628.0]|
# |99.0           |[12.0, 9.0, 48.0, 269.0, 350.0]   |
# |106.0          |[11.0, 0.0, 1.0, 100.0, 89.0]     |
# |120.0          |[0.0, 248.0, 180.0, 133.0, 215.0] |
# +---------------+----------------------------------+


 # songs actual played top rank k
windowSpec = Window.partitionBy('user_id_encoded').orderBy(col('plays').desc())
perUserActualItemsDF = predictions1 \
    .select('user_id_encoded', 'song_id_encoded', 'plays', F.rank().over(windowSpec).alias('rank')) \
    .where('rank <= {0}'.format(k)) \
    .groupBy('user_id_encoded') \
    .agg(expr('collect_list(song_id_encoded) as songs_act'))

perUserItemsRDD = perUserPredictedItemsDF.join(F.broadcast(perUserActualItemsDF), 'user_id_encoded', 'inner') 
rec_act = perUserItemsRDD.select("songs_rec", "songs_act")
rec_act.write.format("parquet").mode("overwrite").options(header="true").options(compression="gzip").save("rec_act")

# load file to RDD format for calculating ranking metrics
perUserItemsRDD = spark.read.parquet("rec_act").rdd
# perUserItemsRDD.isEmpty()
rankingMetrics = RankingMetrics(perUserItemsRDD .map(lambda row: (row[0], row[1])))

rankingMetrics.precisionAt(5)   # 0.6421066406256404

rankingMetrics.meanAveragePrecision #   0.3977777980214467

rankingMetrics.ndcgAt(5)   #   0.6580845953568106






k=10
#-------

# songs predicted top rank k
windowSpec = Window.partitionBy('user_id_encoded').orderBy(F.col('prediction').desc())
perUserPredictedItemsDF = predictions1 \
    .select('user_id_encoded', 'song_id_encoded', 'prediction', F.rank().over(windowSpec).alias('rank')) \
    .where('rank <= {0}'.format(k)) \
    .groupBy('user_id_encoded') \
    .agg(expr('collect_list(song_id_encoded) as songs_rec'))
perUserPredictedItemsDF.show(10,False)

 
# +---------------+----------------------------------+
# |user_id_encoded|songs_rec                         |
# +---------------+----------------------------------+
# |6.0            |[221.0, 289.0, 94.0, 366.0, 40.0] |
# |12.0           |[60.0, 290.0, 64.0, 361.0, 462.0] |
# |13.0           |[15.0, 39.0, 200.0, 57.0, 279.0]  |
# |58.0           |[152.0, 724.0, 18.0, 135.0, 291.0]|
# |79.0           |[46.0, 41.0, 60.0, 215.0, 47.0]   |
# |87.0           |[193.0, 48.0, 148.0, 707.0, 717.0]|
# |93.0           |[93.0, 164.0, 427.0, 698.0, 628.0]|
# |99.0           |[12.0, 9.0, 48.0, 269.0, 350.0]   |
# |106.0          |[11.0, 0.0, 1.0, 100.0, 89.0]     |
# |120.0          |[0.0, 248.0, 180.0, 133.0, 215.0] |
# +---------------+----------------------------------+


 # songs actual played top rank k
windowSpec = Window.partitionBy('user_id_encoded').orderBy(col('plays').desc())
perUserActualItemsDF = predictions1 \
    .select('user_id_encoded', 'song_id_encoded', 'plays', F.rank().over(windowSpec).alias('rank')) \
    .where('rank <= {0}'.format(k)) \
    .groupBy('user_id_encoded') \
    .agg(expr('collect_list(song_id_encoded) as songs_act'))

perUserItemsRDD = perUserPredictedItemsDF.join(F.broadcast(perUserActualItemsDF), 'user_id_encoded', 'inner') 
rec_act = perUserItemsRDD.select("songs_rec", "songs_act")
rec_act.write.format("parquet").mode("overwrite").options(header="true").options(compression="gzip").save("rec_act")

# load file to RDD format for calculating ranking metrics
perUserItemsRDD = spark.read.parquet("rec_act").rdd
# perUserItemsRDD.isEmpty()
rankingMetrics = RankingMetrics(perUserItemsRDD .map(lambda row: (row[0], row[1])))

rankingMetrics.precisionAt(10)   #  0.7618929223964633

rankingMetrics.meanAveragePrecision #  0.6755408901582552


rankingMetrics.ndcgAt(10)   #   0.8630137565200079



 


