package com.bbva.poc.databricks.eventhubsreader

import org.apache.spark.eventhubs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object EventHubsTwitterReader extends App {

  // Build connection string with the above information
  val namespaceName = "<EVENT HUBS NAMESPACE>"
  val eventHubName = "<EVENT HUB NAME>"
  val sasKeyName = "<POLICY NAME>"
  val sasKey = "<POLICY KEY>"
  val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
    .setNamespaceName(namespaceName)
    .setEventHubName(eventHubName)
    .setSasKeyName(sasKeyName)
    .setSasKey(sasKey)

  val customEventhubParameters =
    EventHubsConf(connStr.toString())
      .setMaxEventsPerTrigger(5)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL twitter")
    .config("spark.master", "MASTER-URL")
    .getOrCreate()

  val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

  incomingStream.printSchema

  // Sending the incoming stream into the console.
  // Data comes in batches!
  incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

  // Event Hub message format is JSON and contains "body" field
  // Body is binary, so we cast it to string to see the actual content of the message
  val messages =
  incomingStream
    .withColumn("Offset", col("offset").cast(LongType))
    .withColumn("Time (readable)", col("enqueuedTime").cast(TimestampType))
    .withColumn("Timestamp", col("enqueuedTime").cast(LongType))
    .withColumn("Body", col("body").cast(StringType))
    .select("Offset", "Time (readable)", "Timestamp", "Body")

  messages.printSchema

  messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
}
