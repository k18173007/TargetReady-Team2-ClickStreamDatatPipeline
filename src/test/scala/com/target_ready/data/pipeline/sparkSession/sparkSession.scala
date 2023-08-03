package com.target_ready.data.pipeline.sparkSession

import org.apache.spark.sql.SparkSession
import  com.target_ready.data.pipeline.constants.PipelineConstants.{APP_NAME,MASTER_SERVER}
import com.target_ready.data.pipeline.exceptions.sparkSessionException


object sparkSession {
  def createSparkSession(): SparkSession = {
      SparkSession.builder()
        .appName(APP_NAME)
        .master(MASTER_SERVER)
        .getOrCreate()
  }
}
