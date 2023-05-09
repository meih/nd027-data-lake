from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
sc = spark.sparkContext


timetable_data = "transportation/StationTimeTable1.json"

# df_timetable = spark.read.json(sc.parallelize(timetable_data), multiLine=True)
df_timetable = spark.read.json(timetable_data, multiLine=True)
df_timetable.printSchema()

# flatten the nested data array
df = df_timetable.select(
                "@id",
               "odpt:railDirection",
               "odpt:railway",
               "odpt:station",
               "owl:sameAs",
                F.explode("odpt:stationTimetableObject").alias("data"))

df.show()

# df.createOrReplaceTempView("timetable")
# query = """
# SELECT * FROM timetable
# FILTER odpt:station like'odpt:Station:Toei%'
# """
# sqlDF = spark.sql(query)

# select the necessary columns
df = df.select("@id",
               "odpt:railDirection",
               "odpt:railway",
               "odpt:station",
               "data.odpt:departureTime",
               "data.odpt:departureTime")

# show the resulting table
# df.show()
