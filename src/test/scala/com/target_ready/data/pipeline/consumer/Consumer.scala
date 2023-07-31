package com.target_ready.data.pipeline.consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger._
object Consumer {
  def main(args: Array[String]) {

   /** Creating Spark Session */

    val spark = SparkSession.builder()
      .appName("TargetReady-Team2-ClickStreamDatatPipeline")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    /** Subscribing to the topic and reading data from stream */

    val df = spark                                                                   /** Spark Session */
      .readStream                                                                    /** DataStream Reader*/
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "clickStream")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()                                                                        /** Converting StreamData into DataFrame */


      /** Casting Dataframe into Dataset [(String, String)] format */

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]


      /** Saving the Streamed Data */

      df.writeStream                                                                  /** DataStream Writer */
      .outputMode("append")
      .format("orc")
      .option("path", "data/output")                                                  /** Directory path for saving the data */
      .option("checkpointLocation", "FQDN")
      .trigger(ProcessingTime("30 seconds"))                                  /** Refreshing the Consumer in fixed time Interval */
      .start()
      .awaitTermination()                                                             /** Stopping the Termination */

  }
}







