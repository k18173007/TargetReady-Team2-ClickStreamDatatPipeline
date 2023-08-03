package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.services.readFileService._
import com.target_ready.data.pipeline.services.writeFileService._
import com.target_ready.data.pipeline.clenser.clenser._
import org.apache.spark.sql.{DataFrame,SparkSession}
import com.target_ready.data.pipeline.constants.PipelineConstants._
import com.target_ready.data.pipeline.dqCheck.dqCheckMethods._

object pipelineService {

  def executePipeline() (implicit spark:SparkSession) : Unit = {

    /** ==============================================================================================================
     *                            Reading the data from source directory (.csv file)
     *  ============================================================================================================ */

    val ITEM_DATA: DataFrame = readFile(INPUT_FILE_PATH, INPUT_FORMAT)(spark)


    /** ==============================================================================================================
     *                       Concatenating the data columns into one single columns as value
     *  ============================================================================================================ */

    val CONCATENATED_ITEM_DATA = concatenateColumns(ITEM_DATA,COLUMN_NAMES)


    /** ==============================================================================================================
     *                             Sending the dataframe into kafka topic: writeStream
     *  ============================================================================================================ */

    writeDataToStream(CONCATENATED_ITEM_DATA, TOPIC_NAME)


    /** ==============================================================================================================
     *                            Subscribing to the topic and reading data from stream
     *  ============================================================================================================ */

    val df = loadDataFromStream(TOPIC_NAME)(spark)


    /** ==============================================================================================================
     *                          Splitting Dataframe value-column-data into Multiple Columns
     *  ============================================================================================================ */

    val SPLIT_DATA_DF: DataFrame = splitColumns(COLUMN_NAMES, df)


    /** ==============================================================================================================
     *                                    Converting SPLIT_DATA_DF to UPPERCASE
     *  ============================================================================================================ */4

    val UPPERCASE_DF = uppercaseColumns(SPLIT_DATA_DF)


    /** ==============================================================================================================
     *                                             Trimming UPPERCASE_DF
     *  ============================================================================================================ */

    val TRIMMED_DF = trimColumn(UPPERCASE_DF)


    /** ==============================================================================================================
     *                                   Removing null value rows from TRIMMED_DF
     *  ============================================================================================================ */

    val REMOVED_NULL_VAL_DF = findNullKeys(TRIMMED_DF, ITEM_ID)


    /** ==============================================================================================================
     *                                 Removing duplicate rows from REMOVED_NULL_VAL_DF
     *  ============================================================================================================ */

    //    data = removeDuplicates(df,COLUMNS_PRIMARY_KEY_CLICKSTREAM,Some(EVENT_TIMESTAMP_OPTION))


    /** ==============================================================================================================
     *                                   Converting REMOVED_NULL_VAL_DF to LOWERCASE
     *  ============================================================================================================ */

    val LOWERCASE_DF = lowercaseColumns(REMOVED_NULL_VAL_DF)


    /** ==============================================================================================================
     *                                   Saving LOWERCASE_DF to output dir in required format (.orc)
     *  ============================================================================================================ */

    writeDataToOutputDir(LOWERCASE_DF, OUTPUT_FORMAT, OUTPUT_FILE_PATH)
  }
}
