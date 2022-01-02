# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
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

audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

audio_dataset_names = [
  "msd-jmir-area-of-moments-all-v1.0",
  "msd-jmir-lpc-all-v1.0",
  "msd-jmir-methods-of-moments-all-v1.0",
  "msd-jmir-mfcc-all-v1.0",
  "msd-jmir-spectral-all-all-v1.0",
  "msd-jmir-spectral-derivatives-all-all-v1.0",
  "msd-marsyas-timbral-v1.0",
  "msd-mvd-v1.0",
  "msd-rh-v1.0",
  "msd-rp-v1.0",
  "msd-ssd-v1.0",
  "msd-trh-v1.0",
  "msd-tssd-v1.0"
]

audio_dataset_schemas = {}
for audio_dataset_name in audio_dataset_names:
  print(audio_dataset_name)

  audio_dataset_path = f"/scratch-network/courses/2020/DATA420-20S2/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
  with open(audio_dataset_path, "r") as f:
    rows = [line.strip().split(",") for line in f.readlines()]

  # you could rename feature columns with a short generic name

  # rows[-1][0] = "track_id"
  # for i, row in enumerate(rows[0:-1]):
  #   row[0] = f"feature_{i:04d}"

  audio_dataset_schemas[audio_dataset_name] = StructType([
    StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows
  ])
feature  = spark.read.csv('/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.*', schema=audio_dataset_schemas['msd-jmir-methods-of-moments-all-v1.0'])
#feature.show()
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+
# |Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_4|Method_of_Moments_Overall_Average_5|         MSD_TRACKID|
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+
# |                                        0.1545|                                         13.11|                                         840.0|                                       41080.0|                                     7108000.0|                              0.319|                              33.41|                             1371.0|                            64240.0|                          8398000.0|'TRHFHQZ12903C9E2D5'|
# |                                        0.1195|                                         13.02|                                         611.9|                                       43880.0|                                     7226000.0|                             0.2661|                              30.26|                             1829.0|                           183800.0|                            3.123E7|'TRHFHYX12903CAF953'|
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+

# Q1(a)Produce descriptive statistics for each feature column in the dataset you picked. Are any
# features strongly correlated?

feature.describe().toPandas().transpose()

statistics = (
    feature
    .drop("MSD_TRACKID")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
# print(statistics)
                                    
                                                 # count                 mean               stddev        min       max
# Method_of_Moments_Overall_Standard_Deviation_1  994623   0.1549817600174655  0.06646213086143041        0.0     0.959
# Method_of_Moments_Overall_Standard_Deviation_2  994623   10.384550576951835   3.8680013938747018        0.0     55.42
# Method_of_Moments_Overall_Standard_Deviation_3  994623    526.8139724398112    180.4377549977511        0.0    2919.0
# Method_of_Moments_Overall_Standard_Deviation_4  994623    35071.97543290272   12806.816272955532        0.0  407100.0
# Method_of_Moments_Overall_Standard_Deviation_5  994623    5297870.369577217   2089356.4364557962        0.0   4.657E7
# Method_of_Moments_Overall_Average_1             994623   0.3508444432531261   0.1855795683438387        0.0     2.647
# Method_of_Moments_Overall_Average_2             994623    27.46386798784021    8.352648595163698        0.0     117.0
# Method_of_Moments_Overall_Average_3             994623   1495.8091812075486   505.89376391902437        0.0    5834.0
# Method_of_Moments_Overall_Average_4             994623   143165.46163257837   50494.276171032136  -146300.0  452500.0
# Method_of_Moments_Overall_Average_5             994623  2.396783048473542E7    9307340.299219608        0.0   9.477E7


 
# all_spectral.approxQuantile("Area_Method_of_Moments_Overall_Standard_Deviation_1", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)
# [0.0, 0.8673, 1.187, 1.505, 9.346]
#correlation
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler


assembler = VectorAssembler(
    inputCols=[col for col in feature.columns if col.startswith("Method")],
    outputCol="Features"
)
feature1 = assembler.transform(feature).select(["MSD_TRACKID", "Features"])
# feature1.cache()
# feature1.count()   # 994623

# feature1.show(2, 100)
# +--------------------+---------------------------------------------------------------------------+
# |         MSD_TRACKID|                                                                   Features|
# +--------------------+---------------------------------------------------------------------------+
# |'TRHFHQZ12903C9E2D5'|[0.1545,13.11,840.0,41080.0,7108000.0,0.319,33.41,1371.0,64240.0,8398000.0]|
# |'TRHFHYX12903CAF953'|[0.1195,13.02,611.9,43880.0,7226000.0,0.2661,30.26,1829.0,183800.0,3.123E7]|
# +--------------------+---------------------------------------------------------------------------+


correlations = Correlation.corr(feature1, 'Features', 'pearson')
correlations = Correlation.corr(feature1, 'Features', 'pearson').collect()[0][0].toArray()
 # array([[ 1.        ,  0.42628035,  0.29630589,  0.06103865, -0.05533585,
         # 0.75420787,  0.49792898,  0.44756461,  0.16746557,  0.10040744],
       # [ 0.42628035,  1.        ,  0.85754866,  0.60952091,  0.43379677,
         # 0.02522827,  0.40692287,  0.39635353,  0.01560657, -0.04090215],
       # [ 0.29630589,  0.85754866,  1.        ,  0.80300965,  0.68290935,
        # -0.08241507,  0.12591025,  0.18496247, -0.08817391, -0.13505636],
       # [ 0.06103865,  0.60952091,  0.80300965,  1.        ,  0.94224443,
        # -0.3276915 , -0.22321966, -0.15823074, -0.24503392, -0.22087303],
       # [-0.05533585,  0.43379677,  0.68290935,  0.94224443,  1.        ,
        # -0.39255125, -0.35501874, -0.28596556, -0.26019779, -0.21181281],
       # [ 0.75420787,  0.02522827, -0.08241507, -0.3276915 , -0.39255125,
         # 1.        ,  0.54901522,  0.5185027 ,  0.34711201,  0.2785128 ],
       # [ 0.49792898,  0.40692287,  0.12591025, -0.22321966, -0.35501874,
         # 0.54901522,  1.        ,  0.90336675,  0.51649906,  0.4225494 ],
       # [ 0.44756461,  0.39635353,  0.18496247, -0.15823074, -0.28596556,
         # 0.5185027 ,  0.90336675,  1.        ,  0.7728069 ,  0.68564528],
       # [ 0.16746557,  0.01560657, -0.08817391, -0.24503392, -0.26019779,
         # 0.34711201,  0.51649906,  0.7728069 ,  1.        ,  0.9848665 ],
       # [ 0.10040744, -0.04090215, -0.13505636, -0.22087303, -0.21181281,
         # 0.2785128 ,  0.4225494 ,  0.68564528,  0.9848665 ,  1.        ]])


for i in range(0, correlations.shape[0]):
    for j in range(i + 1, correlations.shape[1]):
        if correlations[i, j] > 0.95:
            print((i, j))

# (0, 5)
# (1, 2)
# (2, 3)
# (3, 4)
# (6, 7)
# (7, 8)
# (8, 9)


# (8, 9)

# drop strong correlated columns
feature1 = feature.select(["Method_of_Moments_Overall_Standard_Deviation_1", "Method_of_Moments_Overall_Standard_Deviation_2","Method_of_Moments_Overall_Standard_Deviation_3", "Method_of_Moments_Overall_Standard_Deviation_4", "Method_of_Moments_Overall_Average_2", "Method_of_Moments_Overall_Average_4", "MSD_TRACKID"])
statistics = (
    feature
    .drop("MSD_TRACKID")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)

feature1 = (
    feature
    .drop("Method_of_Moments_Overall_Average_4")
)
 # feature1.show()
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+--------------------+
# |Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_4|         MSD_TRACKID|
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+--------------------+
# |                                        0.1545|                                         13.11|                                       41080.0|                              33.41|                            64240.0|'TRHFHQZ12903C9E2D5'|
# |                                        0.1195|                                         13.02|                                       43880.0|                              30.26|                           183800.0|'TRHFHYX12903CAF953'|
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+--------------------+
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+
# |Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_5|         MSD_TRACKID|
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+
# |                                        0.1545|                                         13.11|                                         840.0|                                       41080.0|                                     7108000.0|                              0.319|                              33.41|                             1371.0|                          8398000.0|'TRHFHQZ12903C9E2D5'|
# |                                        0.1195|                                         13.02|                                         611.9|                                       43880.0|                                     7226000.0|                             0.2661|                              30.26|                             1829.0|                            3.123E7|'TRHFHYX12903CAF953'|
# +----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+
 


#(b) Load the MSD All Music Genre Dataset (MAGD). Visualize the distribution of genres for the songs that were matched.
# Load MAGD

genre_schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("genre", StringType(), True)
])

genres_MAGD = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("inferSchema", "false")
    .schema(genre_schema)
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv")
    .cache()
)
# genres_MAGD.show(2)
# +------------------+--------------+
# |          track_id|         genre|
# +------------------+--------------+
# |TRAAAAK128F9318786|      Pop_Rock|
# |TRAAAAV128F421A322|      Pop_Rock|
# +------------------+--------------+
# genres_MAGD.count()
# 422714
genre_not_mismatched = genres_MAGD.join(mismatches_not_accepted, on="track_id", how="left_anti")
# genre_not_mismatched.count()
 # 415350
 # genre_not_mismatched.show()
# +------------------+--------------+
# |          track_id|         genre|
# +------------------+--------------+
# |TRAAAAK128F9318786|      Pop_Rock|
# |TRAAAAV128F421A322|      Pop_Rock|
# +------------------+--------------+

genre_distribution = genre_not_mismatched.groupBy('genre').agg(count('track_id'))
# genre_distribution.show()

from pyspark.mllib.stat import Statistics
import pandas as pd
import matplotlib.pyplot as plt

plot_genre = genre_not_mismatched.select(col("genre"))\
    .groupBy(col("genre"))\
    .count()\
    .toPandas()
 


# plot the distribution
output_path = os.path.expanduser("~/plots")
fig, ax = plt.subplots(figsize=(15, 8))
a = plt.bar(plot_genre["genre"], plot_genre["count"])
plt.title('Distribution of Genres')
plt.xlabel('Genres')
plt.ylabel('count')
plt.xticks(rotation = 90)

# Save the plots
#fig.savefig(os.path.join('/users/home/yzh352/Genre Distribution.png', f"Genre Distribution"), bbox_inches="tight")
fig.savefig(os.path.join('/users/home/yzh352/Genre Distribution.png'), bbox_inches="tight")


user_unique_song =triplets_not_mismatched.groupBy("user_id").agg(F.count('song_id').alias('song_count'))
user_unique_song.sort('song_count', ascending=False).show(10,False)


# Q1 (c)Merge the genres dataset and the audio features dataset so that every song has a label.

 
#feature2 = feature1.select(F.substring('MSD_TRACKID',2,18).alias('track_id'), 'Features')
feature2 = feature1.withColumn('track_id',substring('MSD_TRACKID',2,18))

feature_genre2 = feature2.join(genre_not_mismatched, on='track_id', how='inner')
  # feature_genre2.show(2)
 
# +------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+--------+
# |          track_id|Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_5|         MSD_TRACKID|   genre|
# +------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+--------+
# |TRAAABD128F429CF47|                                        0.1308|                                         9.587|                                         459.9|                                       27280.0|                                     4303000.0|                             0.2474|                              26.02|                             1067.0|                          8281000.0|'TRAAABD128F429CF47'|Pop_Rock|
# |TRAABPK128F424CFDB|                                        0.1208|                                         6.738|                                         215.1|                                       11890.0|                                     2278000.0|                             0.4882|                              41.76|                             2164.0|                             3.79E7|'TRAABPK128F424CFDB'|Pop_Rock|
# +------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+--------
assembler = VectorAssembler(
    inputCols=[col for col in feature_genre2.columns if col.startswith("Method")],
    outputCol="Features"
)
feature_genre2 = assembler.transform(feature_genre2).select(["track_id", "Features"])
# feature_genre2.show()
# +------------------+--------------------+
# |          track_id|            Features|
# +------------------+--------------------+
# |TRHFHQZ12903C9E2D5|[0.1545,13.11,410...|
# |TRHFHAU128F9341A0E|[0.2326,7.185,198...|
# +------------------+--------------------+
feature_genre22 = feature_genre2.join(genre_not_mismatched, on='track_id', how='inner') 
# feature_genre22.show()
# +------------------+--------------------+--------------+
# |          track_id|            Features|         genre|
# +------------------+--------------------+--------------+
# |TRAAABD128F429CF47|[0.1308,9.587,272...|      Pop_Rock|
# |TRAAADT12903CCC339|[0.08392,7.541,36...|Easy_Listening|
# +------------------+--------------------+--------------+

#Q2(b)Convert the genre column into a column representing if the song is ”Rap” or some other
# genre as a binary label.

# convert genre Rap to 1, other to 0

feature_genre3 = feature_genre22.withColumn('genre', F.when((F.col("genre") == 'Rap'), 1).otherwise(0)) 
feature_genre3.withColumn('genre',F.col('genre').cast(IntegerType()))
# Class balance
(
    feature_genre3
    .groupBy("genre")
    .count()
    .show(2)
)

# +-----+------+
# |genre| count|
# +-----+------+
# |    0|392727|
# |    1| 20566|
# +-----+------+

 

#the percentage of class 1 (Rap)
print( 20566 / (392727+20566))   # 0.04976 

# Q2(c) Split the dataset into training and test sets. 
 
from pyspark.sql.window import *

# Helpers

def print_class_balance(data, name):
    N = data.count()
    counts = data.groupBy("genre").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")

# Exact stratification using Window 
temp = (
    feature_genre3
    .withColumn("id", monotonically_increasing_id())
    .withColumn("Random", rand())
    .withColumn(
        "Row",
        row_number()
        .over(
            Window
            .partitionBy("genre")
            .orderBy("Random")
        )
    )
)
training = temp.where(
    ((col("genre") == 0) & (col("Row") < 392727 * 0.8)) |
    ((col("genre") == 1) & (col("Row") < 20566 * 0.8))
)
training.cache()

test = temp.join(training, on="id", how="left_anti")
test.cache()

training = training.drop("id", "Random", "Row")
test = test.drop("id", "Random", "Row")

print_class_balance(feature_genre3, "feature_genre")
print_class_balance(training, "training")
print_class_balance(test, "test")
 
 

# feature_genre
# 413293
   # genre   count     ratio
# 0      1   20566  0.049761
# 1      0  392727  0.950239

# training
# 330633
   # genre   count     ratio
# 0      1   16452  0.049759
# 1      0  314181  0.950241

# test
# 82660
   # genre  count    ratio
# 0      1   4114  0.04977
# 1      0  78546  0.95023


# -----------
# Sampling
# -----------

# Imports

import numpy as np

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Helpers

def print_binary_metrics(predictions, labelCol="genre", predictionCol="prediction", rawPredictionCol="rawPrediction"):

    total = predictions.count()
    positive = predictions.filter((col(labelCol) == 1)).count()
    negative = predictions.filter((col(labelCol) == 0)).count()
    nP = predictions.filter((col(predictionCol) == 1)).count()
    nN = predictions.filter((col(predictionCol) == 0)).count()
    TP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 1)).count()
    FP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 0)).count()
    FN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 1)).count()
    TN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 0)).count()

    binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol=labelCol, metricName="areaUnderROC")
    auroc = binary_evaluator.evaluate(predictions)

    print('actual total:    {}'.format(total))
    print('actual positive: {}'.format(positive))
    print('actual negative: {}'.format(negative))
    print('nP:              {}'.format(nP))
    print('nN:              {}'.format(nN))
    print('TP:              {}'.format(TP))
    print('FP:              {}'.format(FP))
    print('FN:              {}'.format(FN))
    print('TN:              {}'.format(TN))
    print('precision:       {}'.format(TP / (TP + FP)))
    print('recall:          {}'.format(TP / (TP + FN)))
    print('auroc:           {}'.format(auroc))


# Check stuff is cached

feature_genre3.cache()
training.cache()
test.cache()

# -------------
#(1) No sampling
# -------------

lr = LogisticRegression(featuresCol='Features', labelCol='genre')
lr_model = lr.fit(training)
predictions = lr_model.transform(test)
predictions.cache()

print_binary_metrics(predictions)



 

# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              560
# nN:              82100
# TP:              159
# FP:              401
# FN:              3955
# TN:              78145
# precision:       0.2839285714285714
# recall:          0.038648517258142924
# auroc:           0.846734555814449



# Binary search for tradeoff threshold------------------------------------------------------

threshold = 0.7
def apply_custom_threshold(probability, threshold):
    return int(probability[1] > threshold)

apply_custom_threshold_udf = F.udf(lambda x: apply_custom_threshold(x, threshold), IntegerType())
temp = predictions.withColumn("customPrediction", apply_custom_threshold_udf(F.col("probability")))
total = predictions.count()
nP = temp.filter((F.col('customPrediction') == 1)).count()
nN = temp.filter((F.col('customPrediction') == 0)).count()
TP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 1)).count()
FP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 0)).count()
FN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 1)).count()
TN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 0)).count()

print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
 

 

 
 
 
# num positive: 142
# num negative: 82518
# precision: 0.20422535211267606
# recall: 0.007049100631988332



# ------------
# (2)Downsampling
# ------------

training_downsampled = (
    training
    .withColumn("Random", rand())
    .where((col("genre") != 0) | ((col("genre") == 0) & (col("Random") < 2 * (20566 / 392727))))
)
training_downsampled.cache()

print_class_balance(training_downsampled, "training_downsampled")
 
 

# 49570
   # genre  count     ratio
# 0      1  16452  0.331894
# 1      0  33118  0.668106




lr2 = LogisticRegression(featuresCol='Features', labelCol='genre')
lr_model2 = lr2.fit(training_downsampled)
predictions2 = lr_model2.transform(test)
predictions2.cache()

print_binary_metrics(predictions2)

 
 

# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              11437
# nN:              71223
# TP:              2554
# FP:              8883
# FN:              1560
# TN:              69663
# precision:       0.22331030864737256
# recall:          0.6208070004861449
# auroc:           0.8516289223258873



# Binary search for tradeoff threshold------------------------------------------------------

temp = predictions2.withColumn("customPrediction", apply_custom_threshold_udf(F.col("probability")))
total = predictions2.count()
nP = temp.filter((F.col('customPrediction') == 1)).count()
nN = temp.filter((F.col('customPrediction') == 0)).count()
TP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 1)).count()
FP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 0)).count()
FN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 1)).count()
TN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 0)).count()
print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
 
 
 


# num positive: 5327
# num negative: 77333
# precision: 0.27989487516425754
# recall: 0.3624210014584346



# ------------
#(3) Upsampling
# ------------

# Randomly upsample by exploding a vector of length betwen 0 and n for each row

ratio = 10
n = 20
p = ratio / n  # ratio < n such that probability < 1

def random_resample(x, n, p):
    # Can implement custom sampling logic per class,
    if x == 0:
        return [0]  # no sampling
    if x == 1:
        return list(range((np.sum(np.random.random(n) > p))))  # upsampling
    return []  # drop

random_resample_udf = udf(lambda x: random_resample(x, n, p), ArrayType(IntegerType()))

training_resampled = (
    training
    .withColumn("Sample", random_resample_udf(col("genre")))
    .select(
        col("track_id"),
        col("Features"),
        col("genre"),
        explode(col("Sample")).alias("Sample")
    )
    .drop("Sample")
)

print_class_balance(training_resampled, "training_resampled")
 


# 478827
   # genre   count     ratio
# 0      1  164738  0.344045
# 1      0  314181  0.656147



lr3 = LogisticRegression(featuresCol='Features', labelCol='genre')
lr_model3 = lr3.fit(training_resampled)
predictions3 = lr_model3.transform(test)
predictions3.cache()

print_binary_metrics(predictions3)
 


 
# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              11789
# nN:              70871
# TP:              2585
# FP:              9204
# FN:              1529
# TN:              69342
# precision:       0.2192722029010094
# recall:          0.6283422459893048
# auroc:           0.8513993642300005



# Binary search for tradeoff threshold------------------------------------------------------

apply_custom_threshold_udf = F.udf(lambda x: apply_custom_threshold(x, threshold), IntegerType())
temp = predictions3.withColumn("customPrediction", apply_custom_threshold_udf(F.col("probability")))
total = predictions3.count()
nP = temp.filter((F.col('customPrediction') == 1)).count()
nN = temp.filter((F.col('customPrediction') == 0)).count()
TP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 1)).count()
FP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 0)).count()
FN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 1)).count()
TN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 0)).count()

print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
 
 


# num positive: 5450
# num negative: 77210
# precision: 0.2768807339449541
# recall: 0.3667963052989791

 



# ------------------------
# (4)Observation reweighting
# ------------------------

training_weighted = (
    training
    .withColumn(
        "Weight",
        when(col("genre") == 0, 1.0)
        .when(col("genre") == 1, 10.0)
        .otherwise(1.0)
    )
)

weights = (
    training_weighted
    .groupBy("genre")
    .agg(
        collect_set(col("Weight")).alias("Weights")
    )
    .toPandas()
)
print(weights)

 

   # genre Weights
# 0      0   [1.0]
# 1      1  [10.0]


lr4 = LogisticRegression(featuresCol='Features', labelCol='genre', weightCol="Weight")
lr_model4 = lr4.fit(training_weighted)
predictions4 = lr_model4.transform(test)
predictions4.cache()

print_binary_metrics(predictions4)


# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              11909
# nN:              70751
# TP:              2605
# FP:              9304
# FN:              1509
# TN:              69242
# precision:       0.21874212780250232
# recall:          0.6332036947010209
# auroc:           0.8530953303069596

 


# Binary search for tradeoff threshold------------------------------------------------------

apply_custom_threshold_udf = F.udf(lambda x: apply_custom_threshold(x, threshold), IntegerType())
temp = predictions4.withColumn("customPrediction", apply_custom_threshold_udf(F.col("probability")))
total = predictions4.count()
nP = temp.filter((F.col('customPrediction') == 1)).count()
nN = temp.filter((F.col('customPrediction') == 0)).count()
TP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 1)).count()
FP = temp.filter((F.col('customPrediction') == 1) & (F.col('genre') == 0)).count()
FN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 1)).count()
TN = temp.filter((F.col('customPrediction') == 0) & (F.col('genre') == 0)).count()

print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
 


# num positive: 5253
# num negative: 77407
# precision: 0.28060156101275463
# recall: 0.3582887700534759

# num positive: 5453
# num negative: 77207
# precision: 0.27709517696680724
# recall: 0.3672824501701507



#Q2(d+e) Train each of the three classification algorithms that you chose in part (a).
# Use the test set to compute the compute a range of performance metrics for each model

# ------------------------
# (1)logistic regression
# ------------------------

from pyspark.ml.feature import PCA 
from pyspark.ml.feature import StandardScaler # standarnize the data before PCA

scaler = StandardScaler(
    withMean=True, withStd=True, inputCol="Features", outputCol="scaled"
)
pca = PCA(k=2, inputCol='Features', outputCol='pcaFeature')
lr = LogisticRegression(featuresCol='Features', labelCol='genre', weightCol="Weight")

#standernize upsampled training data

standermodel = scaler.fit(training_downsampled)
standerdata = standermodel.transform(training_downsampled)

#pca preprocessing
pca_preprocess=pca.fit(standerdata)
pca_data=pca_preprocess.transform(standerdata)

#logistic regression to predict
lr = LogisticRegression(featuresCol='Features', labelCol='genre')
lr=lr.fit(pca_data)
prediction_lr =lr.transform(test)

print_binary_metrics(prediction_lr)
 


 

# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              11437
# nN:              71223
# TP:              2554
# FP:              8883
# FN:              1560
# TN:              69663
# precision:       0.22331030864737256
# recall:          0.6208070004861449
# auroc:           0.8516289223258888



# ------------------------
# (2)randomforest
# ------------------------

from pyspark.ml.classification import RandomForestClassifier
rfclassifier= RandomForestClassifier(featuresCol='Features', labelCol='genre')
rfModel = rfclassifier.fit(training_downsampled)
prediction_rf = rfModel.transform(test)
print_binary_metrics(prediction_rf)

 


# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              13075
# nN:              69585
# TP:              2680
# FP:              10395
# FN:              1434
# TN:              68151
# precision:       0.20497131931166349
# recall:          0.6514341273699562
# auroc:           0.8514462002213518



# ------------------------
# (3)GBTClassifier
# ------------------------

from pyspark.ml.classification import GBTClassifier

gbtclassifier= GBTClassifier(featuresCol='Features', labelCol='genre')
gbtModel =gbtclassifier.fit(training_downsampled)
prediction_gbt = gbtModel.transform(test)
print_binary_metrics(prediction_gbt)
 


# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              13860
# nN:              68800
# TP:              2849
# FP:              11011
# FN:              1265
# TN:              67535
# precision:       0.20555555555555555
# recall:          0.6925133689839572
# auroc:           0.8686998837562532


# +------------------------------------------------------------------------------+
# Q3
# +------------------------------------------------------------------------------+

# Q3(b)tuning hyperparameters

from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# ------------------------
# (1)logistic regression
# ------------------------

scaler = StandardScaler(
    withMean=True, withStd=True, inputCol="Features", outputCol="scaled"
)
pca = PCA(k=2, inputCol='Features', outputCol='pcaFeature')

#using pipeline
pipeline = Pipeline(stages=[scaler, pca])
pipeline_model = pipeline.fit(training_downsampled)
train_processed = pipeline_model.transform(training_downsampled)

lr = LogisticRegression(featuresCol='Features', labelCol='genre')
# build a tuning grid
paramGrid = ParamGridBuilder() \
	.addGrid(lr.maxIter, [20, 50, 100]) \
    .addGrid(lr.regParam, [0, 0.1, 0.2]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) \
    .build()
# evaluator object   
evaluator= BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol='genre', metricName="areaUnderROC")
cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=5)
# CV object
cvModel = cv.fit(train_processed)
#predict
prediction_lr_cv = cvModel.transform(test)

print_binary_metrics(prediction_lr_cv)
 
 

# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              11269
# nN:              71391
# TP:              2512
# FP:              8757
# FN:              1602
# TN:              69789
# precision:       0.22291241458869465
# recall:          0.6105979581915411
# auroc:           0.8561167925391064



bestMod = cvModel.bestModel
# print(bestMod.coefficients)

 


# [8.543597982048976,0.4814377569194103,-3.981408986887114e-05,-0.10735369278699548,1.1077953851838542e-05]

[12.330970146979148,0.556137860694468,-0.004391477479791798,-4.05424245634555e-05,1.2409835896068667e-07,-1.7287965522178597,-0.16286250430820123,0.0013375927239420563,2.78]
print(bestMod.explainParam(bestMod.regParam))
 

# regParam: regularization parameter (>= 0) (default: 0.0, current: 0.0)


print(bestMod.explainParam(bestMod.elasticNetParam))
 
 
 
# elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty (default: 0.0, current: 0.0)


print(bestMod.explainParam(bestMod.maxIter))
 


 

# maxIter: maximum number of iterations (>= 0) (default: 100, current: 100)


# ------------------------
# (2)randomforest
# ------------------------
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(featuresCol='Features', labelCol='genre', numTrees=20, maxDepth=5)

paramGrid_rf = ParamGridBuilder() \
	.addGrid(rf.numTrees, [10, 20, 30]) \
    .addGrid(rf.maxDepth, [3, 5, 10]) \
    .build()
evaluator= BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol='genre', metricName="areaUnderROC")
cv_rf = CrossValidator(
    estimator=rf,
    estimatorParamMaps=paramGrid_rf,
    evaluator=evaluator,
    numFolds=5)

cvModel_rf = cv_rf.fit(training_downsampled)
prediction_rf_cv = cvModel_rf.transform(test)

print_binary_metrics(prediction_rf_cv)
 

# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              12877
# nN:              69783
# TP:              2786
# FP:              10091
# FN:              1328
# TN:              68455
# precision:       0.21635474101110508
# recall:          0.6771998055420515
# auroc:           0.8708584227498601



bestMod = cvModel_rf.bestModel
print(bestMod.explainParam(bestMod.numTrees))
# numTrees: Number of trees to train (>= 1) (default: 20, current: 30)

print(bestMod.explainParam(bestMod.maxDepth))

# maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5, current: 10)


# ------------------------
# (3)GBTClassifier
# ------------------------

gbt = GBTClassifier(featuresCol='Features', labelCol='genre', maxIter=20, maxDepth=5)

paramGrid_gbt = ParamGridBuilder() \
	.addGrid(gbt.maxIter, [10,20, 50]) \
    .addGrid(gbt.maxDepth, [3, 5, 10]) \
    .build()
evaluator= BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol='genre', metricName="areaUnderROC")
cv_gbt = CrossValidator(
    estimator=gbt,
    estimatorParamMaps=paramGrid_gbt,
    evaluator=evaluator,
    numFolds=5)

cvModel_gbt = cv_gbt.fit(training_downsampled)

prediction_gbt_cv = cvModel_gbt.transform(test)
print_binary_metrics(prediction_gbt_cv)
 
 

# actual total:    82660
# actual positive: 4114
# actual negative: 78546
# nP:              13138
# nN:              69522
# TP:              2822
# FP:              10316
# FN:              1292
# TN:              68230
# precision:       0.21479677272035316
# recall:          0.6859504132231405
# auroc:           0.8739868918146361

bestMod = cvModel_gbt.bestModel
print(bestMod.explainParam(bestMod.maxIter))

# maxIter: maximum number of iterations (>= 0) (default: 20, current: 50)

 
print(bestMod.explainParam(bestMod.maxDepth))

# maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5, current: 5)

#Q4(b)Convert the genre column into an integer index

genre_group=feature_genre22.groupBy('genre').agg(F.count('genre'))
genre_group.count()   # 21, the dataset has 21 classes.


#create genre names dataframe.
genre_list = [("Stage ",1),("Religious",2),("Vocal",3),("Easy_Listening",4),("Jazz",5),
  ("Electronic",6),("International",7),("Blues",8),("Children",9),("RnB",10),
  ("Rap",11),("Avant_Garde",12),("Latin",13),("Folk",14),("Pop_Rock",15),
  ("Classical",16),("New Age",17),("Country",18),("Comedy_Spoken",19),("Reggae",20),("Holiday",21)]

genre_names = sqlContext.createDataFrame(genre_list, ["genre", "label"])
genre_names.show(22)
# +--------------+-----+
# |         genre|label|
# +--------------+-----+
# |        Stage |    1|
# |     Religious|    2|
# |         Vocal|    3|
# |Easy_Listening|    4|
# |          Jazz|    5|
# |    Electronic|    6|
# | International|    7|
# |         Blues|    8|
# |      Children|    9|
# |           RnB|   10|
# |           Rap|   11|
# |   Avant_Garde|   12|
# |         Latin|   13|
# |          Folk|   14|
# |      Pop_Rock|   15|
# |     Classical|   16|
# |       New Age|   17|
# |       Country|   18|
# | Comedy_Spoken|   19|
# |        Reggae|   20|
# |       Holiday|   21|
# +--------------+-----+

df = feature_genre22.join(genre_names, on='genre', how='inner')
df.show(10,False)
# +------+------------------+------------------------------------------------------------------------------+-----+
# |genre |track_id          |Features                                                                      |label|
# +------+------------------+------------------------------------------------------------------------------+-----+
# |Stage |TRALTWD128F42B9466|[0.1716,8.459,465.8,39000.0,6211000.0,0.246,26.01,1294.0,169400.0,3.104E7]    |1    |
# |Stage |TRBHZMA128F1467335|[0.05234,5.775,352.5,38550.0,6681000.0,0.052,9.811,474.9,69630.0,1.369E7]     |1    |
# |Stage |TRBILXN128F148726C|[0.03309,4.327,271.0,31990.0,5620000.0,0.09131,15.12,756.2,111600.0,2.173E7]  |1    |
# |Stage |TRBOOUV128F92C7CC2|[0.07751,6.372,372.3,38730.0,6493000.0,0.1261,20.24,1202.0,151800.0,2.702E7]  |1    |
# |Stage |TRBVHVC128F426BC5B|[0.04282,5.01,257.6,26770.0,4502000.0,0.09903,24.13,1324.0,172000.0,3.127E7]  |1    |
# |Stage |TRBVORS128F9312BCF|[0.137,9.98,599.7,51980.0,8105000.0,0.188,21.78,1294.0,151100.0,2.622E7]      |1    |
# |Stage |TRCDCFU128F14ACF6B|[0.01009,3.257,247.4,27180.0,4247000.0,0.02268,8.506,451.1,63430.0,1.135E7]   |1    |
# |Stage |TRCHQLS128F4295295|[0.1003,12.94,569.4,28620.0,4152000.0,0.1912,33.71,1847.0,183800.0,3.082E7]   |1    |
# |Stage |TRCKADD128F92DC36D|[0.05323,3.712,247.4,28770.0,5010000.0,0.1169,11.52,640.7,91170.0,1.716E7]    |1    |
# |Stage |TRCPXBJ128F426DF7E|[0.006684,5.017,403.3,36330.0,6578000.0,0.01711,12.04,445.6,41720.0,5961000.0]|1    |
# +------+------------------+------------------------------------------------------------------------------+-----+

# Q4(c)split the dataset to train an test

df.groupBy('label').agg(F.count('label')).sort('label').show(22)
# +-----+------------+
# |label|count(label)|
# +-----+------------+
# |    1|        1603|
# |    2|        8720|
# |    3|        6064|
# |    4|        1523|
# |    5|       17612|
# |    6|       40027|
# |    7|       14047|
# |    8|        6741|
# |    9|         457|
# |   10|       13854|
# |   11|       20566|
# |   12|         998|
# |   13|       17389|
# |   14|        5701|
# |   15|      232995|
# |   16|         541|
# |   17|        3925|
# |   18|       11411|
# |   19|        2051|
# |   20|        6870|
# |   21|         198|
# +-----+------------+


from pyspark.sql.window import *

# Helpers

def print_class_balance(data, name):
    N = data.count()
    counts = data.groupBy("label").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")

# Exact stratification using Window (multi-class variant in comments)
temp = (
    df 
    .withColumn("id", monotonically_increasing_id())
    .withColumn("Random", rand())
    .withColumn(
        "Row",
        row_number()
        .over(
            Window
            .partitionBy("label")
            .orderBy("Random")
        )
    )
)
training = temp.where(
    ((col("label") == 1) & (col("Row") < 1603 * 0.8)) |
    ((col("label") == 2) & (col("Row") < 8720 * 0.8)) |
    ((col("label") == 3) & (col("Row") < 6064 * 0.8)) |
    ((col("label") == 4) & (col("Row") < 1523 * 0.8)) |
    ((col("label") == 5) & (col("Row") < 17612 * 0.8)) |
    ((col("label") == 6) & (col("Row") < 40027 * 0.8)) |
    ((col("label") == 7) & (col("Row") < 14047 * 0.8)) |
    ((col("label") == 8) & (col("Row") < 6741 * 0.8)) |
    ((col("label") == 9) & (col("Row") < 457 * 0.8)) |
    ((col("label") == 10) & (col("Row") < 13854 * 0.8)) |
    ((col("label") == 11) & (col("Row") < 20566 * 0.8)) |
    ((col("label") == 12) & (col("Row") < 998 * 0.8)) |
    ((col("label") == 13) & (col("Row") < 17389 * 0.8)) |
    ((col("label") == 14) & (col("Row") < 5701 * 0.8)) |
    ((col("label") == 15) & (col("Row") < 232995 * 0.8)) |
    ((col("label") == 16) & (col("Row") < 541 * 0.8)) |
    ((col("label") == 17) & (col("Row") < 3925 * 0.8)) |
    ((col("label") == 18) & (col("Row") < 11411 * 0.8)) |
    ((col("label") == 19) & (col("Row") < 2051 * 0.8)) |
    ((col("label") == 20) & (col("Row") < 6870 * 0.8)) |
    ((col("label") == 21) & (col("Row") < 198 * 0.8))

)

training.cache()

test = temp.join(training, on="id", how="left_anti")
test.cache()

training = training.drop("id", "Random", "Row").sort('label')
test = test.drop("id", "Random", "Row").sort('label')

print_class_balance(df, "raw data")
print_class_balance(training, "training")
print_class_balance(test, "test")



# raw data
# 413293
    # label   count     ratio
# 0       9     457  0.001106
# 1      11   20566  0.049761
# 2      17    3925  0.009497
# 3      14    5701  0.013794
# 4       3    6064  0.014672
# 5       4    1523  0.003685
# 6      19    2051  0.004963
# 7       6   40027  0.096849
# 8      20    6870  0.016623
# 9      13   17389  0.042074
# 10     12     998  0.002415
# 11      2    8720  0.021099
# 12      7   14047  0.033988
# 13      5   17612  0.042614
# 14      8    6741  0.016310
# 15     15  232995  0.563753
# 16     10   13854  0.033521
# 17      1    1603  0.003879
# 18     18   11411  0.027610
# 19     16     541  0.001309
# 20     21     198  0.000479

# training
# 330621
    # label   count     ratio
# 0       1    1282  0.003878
# 1       2    6975  0.021097
# 2       3    4851  0.014672
# 3       4    1218  0.003684
# 4       5   14089  0.042614
# 5       6   32021  0.096851
# 6       7   11237  0.033988
# 7       8    5392  0.016309
# 8       9     365  0.001104
# 9      10   11083  0.033522
# 10     11   16452  0.049761
# 11     12     798  0.002414
# 12     13   13911  0.042075
# 13     14    4560  0.013792
# 14     15  186395  0.563772
# 15     16     432  0.001307
# 16     17    3139  0.009494
# 17     18    9128  0.027609
# 18     19    1640  0.004960
# 19     20    5495  0.016620
# 20     21     158  0.000478

# test
# 82672
    # label  count     ratio
# 0       1    321  0.003883
# 1       2   1745  0.021108
# 2       3   1213  0.014672
# 3       4    305  0.003689
# 4       5   3523  0.042614
# 5       6   8006  0.096841
# 6       7   2810  0.033990
# 7       8   1349  0.016317
# 8       9     92  0.001113
# 9      10   2771  0.033518
# 10     11   4114  0.049763
# 11     12    200  0.002419
# 12     13   3478  0.042070
# 13     14   1141  0.013802
# 14     15  46600  0.563673
# 15     16    109  0.001318
# 16     17    786  0.009507
# 17     18   2283  0.027615
# 18     19    411  0.004971
# 19     20   1375  0.016632
# 20     21     40  0.000484



# ----------
# sampling
# ----------

# upsampling classes with low proportion and downsampling classes with high proportion 

train_1= training.where(col('label')==1).sample(True, 12.0, seed = 2020)
train_2= training.where(col('label')==2).sample(True, 2.5, seed = 2020)
train_3= training.where(col('label')==3).sample(True, 3.0, seed = 2020)
train_4= training.where(col('label')==4).sample(True, 12.0, seed = 2020)
train_5= training.where(col('label')==5).sample(True, 1.2, seed = 2020)
train_6= training.where(col('label')==6).sample(False, 0.5, seed = 2020)
train_7= training.where(col('label')==7).sample(True, 1.5, seed = 2020)
train_8= training.where(col('label')==8).sample(True, 3.0, seed = 2020)
train_9= training.where(col('label')==9).sample(True, 43.0, seed = 2020)
train_10= training.where(col('label')==10).sample(True, 1.5, seed = 2020)
train_11= training.where(col('label')==11)
train_12= training.where(col('label')==12).sample(True, 20.0, seed = 2020)
train_13= training.where(col('label')==13).sample(True, 1.2, seed = 2020)
train_14= training.where(col('label')==14).sample(True, 3.7, seed = 2020)
train_15= training.where(col('label')==15).sample(False, 0.1, seed = 2020)
train_16= training.where(col('label')==16).sample(True, 36.0, seed = 2020)
train_17= training.where(col('label')==17).sample(True, 5.0, seed = 2020)
train_18= training.where(col('label')==18).sample(True, 2.0, seed = 2020)
train_19= training.where(col('label')==19).sample(True, 10.0, seed = 2020)
train_20= training.where(col('label')==20).sample(True, 3.0, seed = 2020)
train_21= training.where(col('label')==21).sample(True, 100.0, seed = 2020)

training_sampled= train_1.union(train_2).union(train_3).union(train_4).union(train_5).union(train_6).union(train_7)

training_sampled=training_sampled.union(train_8).union(train_9).union(train_10).union(train_11).union(train_12).union(train_13).union(train_14)

training_sampled=training_sampled.union(train_15).union(train_16).union(train_17).union(train_18).union(train_19).union(train_20).union(train_21)

training_sampled.cache()

def print_class_balance(data, name):
    N = data.count()
    counts = data.groupBy("label").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")

print_class_balance(training_sampled, "training_sampled")
# training_sampled
# 347087
#     label  count     ratio
# 0       9  15727  0.045311
# 1      11  16452  0.047400
# 2      14  16929  0.048775
# 3      17  15890  0.045781
# 4       3  14816  0.042687
# 5       4  14774  0.042566
# 6      19  16627  0.047904
# 7       6  16038  0.046207
# 8      13  16951  0.048838
# 9      20  16759  0.048285
# 10      2  17611  0.050739
# 11     12  16106  0.046403
# 12      7  17135  0.049368
# 13      5  17175  0.049483
# 14      8  16445  0.047380
# 15     15  18937  0.054560
# 16      1  15524  0.044727
# 17     10  16907  0.048711
# 18     16  15681  0.045179
# 19     18  18501  0.053304
# 20     21  16102  0.046392

#randomforest

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

rfclassifier= RandomForestClassifier(featuresCol='Features', labelCol='label')
rfModel = rfclassifier.fit(training_sampled)
prediction_rf = rfModel.transform(test)


evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol='label', metricName='f1')
evaluator.evaluate(prediction_rf)
 
 # 0.35155772504855737

 
 
evaluator.evaluate(prediction_rf, {evaluator.metricName: "weightedPrecision"})
 
# 0.54280982739441


evaluator.evaluate(prediction_rf, {evaluator.metricName: "weightedRecall"})
# 0.2918400425778982




