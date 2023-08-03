package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.exceptions.writeFileException
import com.target_ready.data.pipeline.constants.PipelineConstants.{SERVER_ID}
import org.apache.spark.sql.DataFrame


object writeFileService {

  /** ===============================================================================================================
   *                                  FUNCTION TO WRITE DATA INTO KAFKA STREAM.
   *  ==============================================================================================================*/

  def writeDataToStream(df: DataFrame, topic: String): Unit = {
    try {
      df
        .selectExpr("CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", SERVER_ID)
        .option("topic", topic)
        .save()
    } catch {
      case e: Exception => writeFileException("Unable to write files to the location: " + topic)
    }
  }


  /** ===============================================================================================================
   *                                FUNCTION TO SAVE DATA INTO OUTPUT LOCATION.
   *  ==============================================================================================================*/

  def writeDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", "FQDN")
        .start()
        .awaitTermination()
    } catch {
      case e: Exception => writeFileException("Unable to write files to the location: " + filePath)
    }
  }


  /** ===============================================================================================================
   *                                FUNCTION TO SAVE NULL-VALUE DATA INTO NULL-VALUE-OUTPUT LOCATION.
   *  ============================================================================================================== */

  def writeNullDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", "FQDN")
        .start()
        .awaitTermination(2000)
    } catch {
      case e: Exception => writeFileException("Unable to write files to the location: " + filePath)
    }
  }
}
