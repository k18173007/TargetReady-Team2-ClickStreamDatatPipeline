package com.target_ready.data.pipeline

import com.target_ready.data.pipeline.services.pipelineService
import com.target_ready.data.pipeline.sparkSession.sparkSession.createSparkSession
import org.apache.spark.sql.SparkSession

object DataPipeline {

  def main(args: Array[String]): Unit = {

    /** ==============================================================================================================
     *                                            Creating Spark Session
     *  ============================================================================================================ */

    val spark:SparkSession=createSparkSession()


    /** ==============================================================================================================
     *                                             Executing Pipeline
     *  ============================================================================================================ */

      pipelineService.executePipeline()(spark)

  }
}
