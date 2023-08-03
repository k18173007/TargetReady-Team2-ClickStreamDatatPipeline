package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.services.readFileService._
import com.target_ready.data.pipeline.services.writeFileService._
import com.target_ready.data.pipeline.services.tranformationServices.transformDf
import org.apache.spark.sql.{DataFrame,SparkSession}
import com.target_ready.data.pipeline.constants.PipelineConstants._

object pipelineService {

  def executePipeline() (implicit spark:SparkSession) : Unit = {

    /** ==============================================================================================================
     *                            Reading the data from source directory (.csv file)
     *  ============================================================================================================ */

    val ITEM_DATA_DF: DataFrame = readFile(INPUT_FILE_PATH, INPUT_FORMAT)(spark)


    /** ==============================================================================================================
     *                           Applying transformations on ITEM_DATA_DF
     *  ============================================================================================================ */

    val TRANSFORMED_DATA_DF=transformDf(ITEM_DATA_DF)


    /** ==============================================================================================================
     *              Saving the final transformed data in output location in required output format(.orc)
     *  ============================================================================================================ */

    writeDataToOutputDir(TRANSFORMED_DATA_DF, OUTPUT_FORMAT, OUTPUT_FILE_PATH)
  }
}
