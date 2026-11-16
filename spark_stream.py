
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    hour, minute, second
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.streaming import StreamingQueryListener

spark = (
    SparkSession.builder
    .appName("CampusRealTimeAnalytics")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Set campus timezone to Algeria (UTC+1)
spark.conf.set("spark.sql.session.timeZone", "Africa/Algiers")


# Display Spark processing statistics for each batch
class ProcessingRateListener(StreamingQueryListener):

    def onQueryStarted(self, event):
        print("Streaming query started:", event.name)

    def onQueryProgress(self, event):
        progress = event.progress

        print("\n========== Spark Processing Rate ==========")
        print("Query:", progress.name)
        print("Batch ID:", progress.batchId)
        print("Input rows:", progress.numInputRows)
        print("Input rate:", round(progress.inputRowsPerSecond, 2),
              "events/second")
        print("Processing rate:", round(progress.processedRowsPerSecond, 2),
              "events/second")
        print("===========================================\n")

    def onQueryTerminated(self, event):
        print("Streaming query terminated:", event.id)


spark.streams.addListener(ProcessingRateListener())


schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])


# Read events from Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "earliest")
    .load()
)

json_events = raw.selectExpr("CAST(value AS STRING) AS json")


# Parse JSON events
parsed = (
    json_events
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)


# Query 1: Count events by event type and department
# Sliding window: 2 minutes, advancing every 30 seconds
counts = (
    parsed
    .withWatermark("event_ts", "2 minutes")
    .groupBy(
        window(col("event_ts"), "2 minutes", "30 seconds"),
        col("eventType"),
        col("department")
    )
    .count()
)

query = (
    counts.writeStream
    .queryName("event_type_department_counts")
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/campus-sliding-30sec")
    .start()
)


# Query 2: Detect check-ins after 08:15:00 local campus time
checkins = parsed.filter(col("eventType") == "check-in")

late_checkins = (
    checkins
    .filter(
        (hour(col("event_ts")) > 8) |
        (
            (hour(col("event_ts")) == 8) &
            (
                (minute(col("event_ts")) > 15) |
                (
                    (minute(col("event_ts")) == 15) &
                    (second(col("event_ts")) > 0)
                )
            )
        )
    )
    .select("studentId", "department", "eventTime")
)

alert_query = (
    late_checkins.writeStream
    .queryName("late_checkin_alerts")
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/campus-late-checkins")
    .start()
)


query.awaitTermination()
