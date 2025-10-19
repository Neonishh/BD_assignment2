from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round, date_format, unix_timestamp, lit, explode, array
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Thresholds
CPU_THRESHOLD = 82.53
MEM_THRESHOLD = 81.13

# Initialize Spark Session
spark = SparkSession.builder.appName("CPU_MEM_ALERTS").getOrCreate()

# Paths
cpu_csv = "data/cpu_data.csv"
mem_csv = "data/mem_data.csv"
output_csv = "output/26_CPU_MEM.csv"

# Schema with FloatType 
cpu_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("cpu_pct", FloatType(), True),
])
mem_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("mem_pct", FloatType(), True),
])

# Load CSVs
cpu_df = spark.read.csv(cpu_csv, schema=cpu_schema, header=True)
mem_df = spark.read.csv(mem_csv, schema=mem_schema, header=True)

# Convert ts to timestamp
cpu_df = cpu_df.withColumn("ts", col("ts").cast("timestamp"))
mem_df = mem_df.withColumn("ts", col("ts").cast("timestamp"))

# Join CPU + MEM
joined_df = cpu_df.join(mem_df, ["server_id", "ts"])

# Get min and max per server
server_bounds = joined_df.groupBy("server_id").agg(
    F.min("ts").alias("min_ts"),
    F.max("ts").alias("max_ts")
)

# Convert timestamps to unix
joined_df = joined_df.withColumn("ts_unix", unix_timestamp("ts"))

# Join with server bounds
joined_df = joined_df.join(server_bounds, "server_id")
joined_df = joined_df.withColumn("min_ts_unix", unix_timestamp("min_ts"))
joined_df = joined_df.withColumn("max_ts_unix", unix_timestamp("max_ts"))

# Align per server
joined_df = joined_df.withColumn(
    "aligned_min",
    (F.floor(col("min_ts_unix") / 10) * 10).cast("long")
)
joined_df = joined_df.withColumn(
    "aligned_max",
    (F.floor(col("max_ts_unix") / 10) * 10).cast("long")
)

# Calculate which window starts this record participates in
joined_df = joined_df.withColumn(
    "possible_window_starts",
    array(
        (F.floor((col("ts_unix") - 20) / 10) * 10).cast("long"),
        (F.floor((col("ts_unix") - 10) / 10) * 10).cast("long"),
        (F.floor(col("ts_unix") / 10) * 10).cast("long")
    )
)

# Explode to get one row per window participation
exploded_df = joined_df.select(
    col("server_id"),
    col("ts_unix"),
    col("cpu_pct"),
    col("mem_pct"),
    explode(col("possible_window_starts")).alias("window_start_unix"),
    col("aligned_min"),
    col("aligned_max")
)

# Filter valid windows
exploded_df = exploded_df.filter(
    (col("ts_unix") >= col("window_start_unix")) &
    (col("ts_unix") < col("window_start_unix") + 30) &
    (col("window_start_unix") >= col("aligned_min")) &
    (col("window_start_unix") <= col("aligned_max"))
)

# Convert window boundaries to timestamps
exploded_df = exploded_df.withColumn(
    "window_start",
    col("window_start_unix").cast("timestamp")
)
exploded_df = exploded_df.withColumn(
    "window_end",
    (col("window_start_unix") + 30).cast("timestamp")
)

# Aggregate by window and server
agg_df = exploded_df.groupBy(
    col("server_id"),
    col("window_start"),
    col("window_end")
).agg(
    spark_round(avg("cpu_pct"), 2).alias("avg_cpu"),
    spark_round(avg("mem_pct"), 2).alias("avg_mem")
)

# Alert logic
def alert_logic(avg_cpu, avg_mem):
    if avg_cpu > CPU_THRESHOLD and avg_mem > MEM_THRESHOLD:
        return "High CPU + Memory stress"
    elif avg_cpu > CPU_THRESHOLD and avg_mem <= MEM_THRESHOLD:
        return "CPU spike suspected"
    elif avg_mem > MEM_THRESHOLD and avg_cpu <= CPU_THRESHOLD:
        return "Memory saturation suspected"
    else:
        return ""

alert_udf = udf(alert_logic, StringType())

# Apply UDF
result_df = agg_df.withColumn("alert", alert_udf(col("avg_cpu"), col("avg_mem")))

# Format timestamps and sort
final_df = result_df.select(
    col("server_id"),
    date_format(col("window_start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window_end"), "HH:mm:ss").alias("window_end"),
    col("avg_cpu"),
    col("avg_mem"),
    col("alert")
).orderBy("server_id", "window_start")

# Debug: print count before writing
print(f"Total rows: {final_df.count()}")

# Write output
final_df.write.csv(output_csv, header=True, mode="overwrite")

spark.stop()




