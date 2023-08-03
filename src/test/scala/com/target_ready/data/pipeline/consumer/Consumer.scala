package com.target_ready.data.pipeline.consumer

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.target_ready.data.pipeline.clenser.clenser.{uppercaseColumns,trimColumn,lowercaseColumns,splitColumns}
import com.target_ready.data.pipeline.services.writeFileService.writeDataToOutputDir
import com.target_ready.data.pipeline.services.readFileService.loadDataFromStream
import com.target_ready.data.pipeline.constants.PipelineConstants._
import com.target_ready.data.pipeline.sparkSession.sparkSession.createSparkSession
import com.target_ready.data.pipeline.dqCheck.dqCheckMethods.{findNullKeys}

//object Consumer {
//  def main(args: Array[String]) {
//
//   /** Creating Spark Session */
//    val spark = createSparkSession()
//
//
//    import spark.implicits._
//
//    /** Subscribing to the topic and reading data from stream */
//    val df = loadDataFromStream(TOPIC_NAME)(spark)
//
//    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
//
//    var data:DataFrame=splitColumns(COLUMN_NAMES,ds)
//
//    data=uppercaseColumns(data)
//
//    data=trimColumn(data)
//
//    data=findNullKeys(data,ITEM_ID)
////    data = removeDuplicates(df,COLUMNS_PRIMARY_KEY_CLICKSTREAM,Some(EVENT_TIMESTAMP_OPTION))
//    data=lowercaseColumns(data)
//
//
//    /** Saving the Streamed Data */
//    writeDataToOutputDir(data,OUTPUT_FORMAT,OUTPUT_FILE_PATH)
//
//  }
//}



















import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.functions.{col, from_json, from_csv}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
//This stream will print the output to File (DFS) ( Kafka -> File)
//object KafkaConsumer {
//  def main(args: Array[String]) {
//    // $example on:init_session$
//    val spark = SparkSession
//      .builder()
//      .appName("Kafkaprocon")
//      .master("local")
//      //.config("spark.some.config.option", "some-value")
//      .getOrCreate()
//
//
//    import spark.implicits._
//
//    // Subscribe to 1 topic
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "kafkastream")
//      .option("startingOffsets", "earliest")
//      .load()
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
//      .writeStream
//      .outputMode("append") // default
//      .outputMode("update") // not supported by CSV
//      .format("orc")
//      .option("path", "/tmp/data/output/Clickstream")
//      .option("checkpointLocation", "/tmp/data/output/checkpointFS2F")
//      .trigger(ProcessingTime("30 seconds"))  // only change in query
//      .start()
//      .awaitTermination()
//
//  }
//}











//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{col, from_json}
//import org.apache.spark.sql.types.{IntegerType, StructField, StringType, StructType}
//
//object Consumer {
//  def main(args:Array[String]): Unit= {
//    val spark: SparkSession = SparkSession.builder()
//      .master("local[3]")
//      .appName("Spark1")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("ERROR")
//
//    val df = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "192.168.1.100:9092")
//      .option("subscribe", "test1")
//      .option("startingOffsets", "earliest") // From starting
//      .load()
//
////      .awaitTermination()
//
////    df.printSchema()
//    df.show()
//
////df.awaitTermination()
//    //df.show(false)
//    //org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;
//
////    val schema = StructType(
////      List(
////        StructField("item_id", StringType, true),
////        StructField("item_price", StringType, true),
////        StructField("product_type", StringType, true),
////        StructField("department_name", StringType, true)
////      )
////    )
////
////
////
////    val person = df.selectExpr("CAST(value AS STRING)")
////      .select(from_json(col("value"), schema).as("data"))
////      .select("data.*")
////
////    /**
////     *uncomment below code if you want to write it to console for testing.
////     */
////    //    val query = person.writeStream
////    //      .format("console")
////    //      .outputMode("append")
////    //      .start()
////    //      .awaitTermination()
////
////    /**
////     *uncomment below code if you want to write it to kafka topic.
////     */
////    df.writeStream
////      .format("kafka")
////      .outputMode("append")
////      .option("kafka.bootstrap.servers", "192.168.1.100:9092")
////      .option("topic", "test1")
////      .option("checkpointLocation","FQDN")
////      .start()
////      .awaitTermination()
////    df.show()
//  }
//}
//
