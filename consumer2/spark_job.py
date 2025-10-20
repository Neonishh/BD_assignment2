from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, round as spark_round, date_format, unix_timestamp, explode, array, floor
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

NET_THRESHOLD = 4570.32
DISK_THRESHOLD = 2906.97

spark = SparkSession.builder.appName("NET_DISK_ALERTS").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

net_csv = "data/net_data.csv"
disk_csv = "data/disk_data.csv"
output_csv = "output/26_NET_DISK.csv"

net_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("max_net_in", FloatType(), True),
])

disk_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("max_disk_io", FloatType(), True),
])

net_df = spark.read.csv(net_csv, schema=net_schema, header=True)
disk_df = spark.read.csv(disk_csv, schema=disk_schema, header=True)

net_df = net_df.withColumn("ts", col("ts").cast("timestamp"))
disk_df = disk_df.withColumn("ts", col("ts").cast("timestamp"))

joined_df = net_df.join(disk_df, ["server_id", "ts"])

server_bounds = joined_df.groupBy("server_id").agg(
    F.min("ts").alias("min_ts"),
    F.max("ts").alias("max_ts")
)

joined_df = joined_df.withColumn("ts_unix", spark_round(unix_timestamp("ts"), 2))
joined_df = joined_df.join(server_bounds, "server_id")
joined_df = joined_df.withColumn("min_ts_unix", spark_round(unix_timestamp("min_ts"), 2))
joined_df = joined_df.withColumn("max_ts_unix", spark_round(unix_timestamp("max_ts"), 2))

joined_df = joined_df.withColumn(
    "aligned_min",
    (floor(col("min_ts_unix") / 10) * 10).cast("long")
)
joined_df = joined_df.withColumn(
    "aligned_max",
    (floor(col("max_ts_unix") / 10) * 10).cast("long")
)

joined_df = joined_df.withColumn(
    "possible_window_starts",
    array(
        (floor((col("ts_unix") - 20) / 10) * 10).cast("long"),
        (floor((col("ts_unix") - 10) / 10) * 10).cast("long"),
        (floor(col("ts_unix") / 10) * 10).cast("long")
    )
)

exploded_df = joined_df.select(
    col("server_id"),
    col("ts_unix"),
    col("max_net_in"),
    col("max_disk_io"),
    explode(col("possible_window_starts")).alias("window_start_unix"),
    col("aligned_min"),
    col("aligned_max")
)

exploded_df = exploded_df.filter(
    (col("ts_unix") >= col("window_start_unix")) &
    (col("ts_unix") < col("window_start_unix") + 30) &
    (col("window_start_unix") >= col("aligned_min")) &
    (col("window_start_unix") <= col("aligned_max"))
)

exploded_df = exploded_df.withColumn(
    "window_start",
    col("window_start_unix").cast("timestamp")
).withColumn(
    "window_end",
    (col("window_start_unix") + 30).cast("timestamp")
)

agg_df = exploded_df.groupBy(
    col("server_id"),
    col("window_start"),
    col("window_end")
).agg(
    spark_round(spark_max("max_net_in"), 2).alias("max_net_in"),
    spark_round(spark_max("max_disk_io"), 2).alias("max_disk_io")
)

def alert_logic(max_net_in, max_disk_io):
    if max_net_in > NET_THRESHOLD and max_disk_io > DISK_THRESHOLD:
        return "Network flood + Disk thrash suspected"
    elif max_net_in > NET_THRESHOLD and max_disk_io <= DISK_THRESHOLD:
        return "Possible DDoS"
    elif max_disk_io > DISK_THRESHOLD and max_net_in <= NET_THRESHOLD:
        return "Disk thrash suspected"
    else:
        return ""

alert_udf = udf(alert_logic, StringType())

result_df = agg_df.withColumn("alert", alert_udf(col("max_net_in"), col("max_disk_io")))

final_df = result_df.select(
    col("server_id"),
    date_format(col("window_start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window_end"), "HH:mm:ss").alias("window_end"),
    col("max_net_in"),
    col("max_disk_io"),
    col("alert")
).orderBy("server_id", "window_start")

print(f"Total rows: {final_df.count()}")

final_df.write.csv(output_csv, header=True, mode="overwrite")

spark.stop()
