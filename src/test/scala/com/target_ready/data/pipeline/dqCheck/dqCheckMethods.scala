package com.target_ready.data.pipeline.dqCheck

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, desc, row_number, when}

/** ===============================================================================================================
 * DQ CHECK METHODS
 * Ignore for now (Changes have to be made)
 *  ============================================================================================================== */

object dqCheckMethods {
//  def findNullKeys(df: DataFrame, column: String): DataFrame = {
//    var resultDf = df
//
//    //    val nullKeyDataDf:DataFrame = df.filter(col(column).isNull || col(column) === "")
//    val nullKeyDataDf: DataFrame = df.filter(col(column).isNull || col(column) === "")
//    //    writeNullDataToOutputDir(nullKeyDataDf,NULL_VALUE_FILE_FORMAT,NULL_VALUE_PATH)
//
//    //    val nullValueCount:Long=nullKeyDataDf.count()
//    //    if(nullValueCount>0){
//    //      throw dqNullCheckException("Input file contains NULL values.")
//    //    }
//    val notNullKeyData = df.filter(col(column).isNotNull && !(col(column) <=> lit("")))
//    resultDf = notNullKeyData
//    resultDf
//  }


//    def dropDuplicates(df: DataFrame, KeyColumns: Seq[String], orderByCol: String): Boolean = {
//      val windowSpec = Window.partitionBy(KeyColumns.map(col): _*).orderBy(desc(orderByCol))
//      val dfDropDuplicate: DataFrame = df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
//        .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
//
//      true
//
//  }
}
