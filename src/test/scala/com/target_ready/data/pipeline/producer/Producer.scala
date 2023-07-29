package com.target_ready.data.pipeline.producer


/**=================================================================================================
 *                 THIS PACKAGE CONTAINS TWO METHODS TO SEND DATA INTO KAFKA TOPIC
 *=================================================================================================*/



/**======================== METHOD 1: By Creating Spark Session ======================================*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Producer {
  def main(args: Array[String]): Unit = {

    /** Creating Spark Session */
    val spark = SparkSession.builder()
      .appName("TargetReady-Team2-ClickStreamDatatPipeline")
      .master("local[*]")
      .getOrCreate()

    //    /** SCHEMA: comment it out in-case you want to drop header of the dataframe */
    //    val schema = StructType(
    //      List(
    //        StructField("item_id", StringType, true),
    //        StructField("item_price", StringType, true),
    //        StructField("product_type", StringType, true),
    //        StructField("department_name", StringType, true)
    //      )
    //    )

    /** Reading the data from source directory (.csv file) */
    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("data/Input/item/item_data.csv")


    /** Concatenating the data columns into one single columns as value */
    val df = data
      .select(concat(col("item_id"), lit(','),
        col("item_price"), lit(','),
        col("product_type"), lit(","),
        col("department_name"))
        .as("value"))


    /** Sending the dataframe into kafka topic: writeStream */
    val query = df
      .selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "writeStream")
      .save()

  }
}






/**======================== METHOD 2: By Using kafka producer =======================================*/

//import java.util.Properties
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
//import org.apache.kafka.common.serialization.StringSerializer
//import scala.io.Source
//
//object Producer {
//  def main(args: Array[String]): Unit = {
//
//    /** Setting the Configuration properties for kafka producer */
//    val config: Properties = new Properties()
//    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//
//    /** Creating Kafka Producer Object */
//    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)
//
//    /** Input file name  */
//    val fileName = "C://target_ready//Phase 2//TargetReady-Team2-ClickStreamDatatPipeline//data//Input//item/item_data.csv"
//
//    /** Kafka Topic Name */
//    val topicName = "clickStream"
//
//
//    /** The LOOP reads every line of .csv file
//     * (EXCEPT HEADER) and send it to the Topic
//     * with its Corresponding key */
//
//    for (line <- Source.fromFile(fileName).getLines().drop(1)) {  /** Droping the Header */
//
//      val key = line.split(",") {0} /** Extracting key from every line */
//
//      /** Preparing the data */
//      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, line)
//
//      /** Send to topic */
//      producer.send(record)
//
//    }
//
//    /** Flushing the Producer (blocking it until previous messaged
//     * have been delivered effectively, to make it synchronous) */
//    producer.flush()
//
//    /** Closing the Producer */
//    producer.close()
//
//  }
//}


