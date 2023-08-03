package com.target_ready.data.pipeline.producer


/**=================================================================================================
 *                 THIS PACKAGE CONTAINS TWO METHODS TO SEND DATA INTO KAFKA TOPIC
 *=================================================================================================*/



/**======================== METHOD 1: By Creating Spark Session ======================================*/
import com.target_ready.data.pipeline.sparkSession.sparkSession.createSparkSession
import com.target_ready.data.pipeline.services.readFileService.readFile
import com.target_ready.data.pipeline.services.writeFileService.writeDataToStream
import com.target_ready.data.pipeline.clenser.clenser.concatenateColumns
import org.apache.spark.sql.DataFrame
import com.target_ready.data.pipeline.constants.PipelineConstants.{INPUT_FILE_PATH,INPUT_FORMAT,TOPIC_NAME}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

//object Producer {
//  def main(args: Array[String]): Unit = {
//
//    /** Creating Spark Session */
//    val spark= createSparkSession()
//
//    /** Reading the data from source directory (.csv file) */
//
//    val data:DataFrame = readFile(INPUT_FILE_PATH,INPUT_FORMAT)(spark)
////    logInfo("Item data read from input location complete.")
//
//
//    /** Concatenating the data columns into one single columns as value */
//    val df = concatenateColumns(data)
//
//
//    /** Sending the dataframe into kafka topic: writeStream */
//
//    writeDataToStream(df,TOPIC_NAME)
//  }
//}






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
//    val fileName = "data//Input//item/Test_data.csv"
//
//    /** Kafka Topic Name */
////    val topicName = "writeStreamTest"
////    val topicName = "NullDataTestStream"
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
//      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](TOPIC_NAME, key, line)
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


