package com.target_ready.data.pipeline.clenser

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, _}

object clenser {

  /** ==============================================================================================================
   *                                   FUNCTION TO UPPERCASE DATAFRAME COLUMNS
   *  ============================================================================================================ */

  def uppercaseColumns(df: DataFrame): DataFrame = {
    val columns = df.columns
    var resultDf = df

    for (colm <- columns)  resultDf = resultDf.withColumn(colm, upper(col(colm)))
    resultDf
  }


  /** ==============================================================================================================
   *                                   FUNCTION TO LOWERCASE DATAFRAME COLUMNS
   *  ============================================================================================================ */

  def lowercaseColumns(df: DataFrame): DataFrame = {
    val columns = df.columns
    var resultDf = df

    for (colm <- columns)  resultDf = resultDf.withColumn(colm, lower(col(colm)))
    resultDf
  }


  /** ===============================================================================================================
   *                                     FUNCTION TO TRIM DATAFRAME COLUMNS
   *  ============================================================================================================ */

  def trimColumn(df: DataFrame): DataFrame = {
    val columns = df.columns
    var resultDf = df

    for (colm <- columns) {
      resultDf = df.withColumn(colm, trim(col(colm)))
      resultDf = df.withColumn(colm, ltrim(col(colm)))
      resultDf = df.withColumn(colm, rtrim(col(colm)))
    }
    resultDf
  }

  /** ==============================================================================================================
   *                    FUNCTION TO REMOVE DUPLICATE ROWS IN DATAFRAME
   *  ============================================================================================================ */

  //  def removeDuplicates(df: DataFrame,primaryKeyColumns: Seq[String], orderByColumn: Option[String]): DataFrame = {
  //
  //      val dfDropDuplicates: DataFrame = Window.partitionBy(primaryKeyColumns.map(col): _*).orderBy(desc(orderCol))
  //        df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
  //          .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
  //
  //    dfDropDuplicates
  //  }


  /** ==============================================================================================================
   *                       FUNCTION TO SPLIT READ-STREAM DATAFRAME COLUMN(Value) TO MULTIPLE COLUMNS
   *  ============================================================================================================ */

  def splitColumns(Col: List[String], ds: DataFrame): DataFrame = {
    val res = ds.select(split(col("value"), ",").getItem(0).as(Col(0)),
      split(col("value"), ",").getItem(1).as(Col(1)),
      split(col("value"), ",").getItem(2).as(Col(2)),
      split(col("value"), ",").getItem(3).as(Col(3)))
    res
  }


  /** ==============================================================================================================
   *                       FUNCTION TO CONCATENATE READ-STREAM DATAFRAME COLUMN(Value) TO MULTIPLE COLUMNS
   *  =========================================================================================================== */

  def concatenateColumns(df: DataFrame,Columns:List[String]): DataFrame = {
    val resultDf = df.select(concat(col(Columns(0)), lit(','),
      col(Columns(1)), lit(','),
      col(Columns(2)), lit(","),
      col(Columns(3)))
      .as("value"))
    resultDf
  }


  /** ==============================================================================================================
   *                        FUNCTION TO FIND AND REMOVE NULL VALUE ROWS FROM DATAFRAME
   *  =========================================================================================================== */
  def findNullKeys(df: DataFrame, column: String): DataFrame = {
    var resultDf = df

    val nullKeyDataDf: DataFrame = df.filter(col(column).isNull || col(column) === "")

    val notNullKeyData = df.filter(col(column).isNotNull && !(col(column) <=> lit("")))
    resultDf = notNullKeyData
    resultDf
  }

}
