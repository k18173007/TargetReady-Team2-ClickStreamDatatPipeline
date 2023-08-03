package com.target_ready.data.pipeline.constants

object PipelineConstants {

  /** ===============================================================================================================
   *                                               SERVER CONFIG PROPS
   *  ============================================================================================================== */

  val SERVER_ID: String = "localhost:9092"
  val APP_NAME: String = "TargetReady-Team2-ClickStreamDatatPipeline"
  val MASTER_SERVER: String = "local[*]"
  val TOPIC_NAME: String = "TestStream2"


  /** ===============================================================================================================
   *                                             INPUT-OUTPUT formats, paths
   *  ============================================================================================================== */

  val INPUT_FORMAT: String = "csv"
  val OUTPUT_FORMAT: String = "csv"
  val INPUT_FILE_PATH: String = "data/Input/item/Test_data.csv"
  val OUTPUT_FILE_PATH: String = "data/output"
  val NULL_VALUE_PATH: String = "data/null_value_output"
  val NULL_VALUE_FILE_FORMAT: String = "csv"


  /** ===============================================================================================================
   *                                                DATAFRAME COLUMN NAMES
   *  ============================================================================================================== */

  val ITEM_ID: String = "item_id"
  val ITEM_PRICE: String = "item_price"
  val PRODUCT_TYPE: String = "product_type"
  val DEPARTMENT_NAME: String = "department_name"
  val ROW_NUMBER: String = "row_number"
  val SESSION_ID: String = "session_id"
  val VISITOR_ID: String = "visitor_id"
  val VALUE: String = "value"
  val COLUMNS_PRIMARY_KEY_CLICK_STREAM: Seq[String] = Seq(PipelineConstants.VALUE)
  val EVENT_TIMESTAMP_OPTION: String = "timestamp"
  val COLUMN_NAMES: List[String] = List(ITEM_ID, ITEM_PRICE, PRODUCT_TYPE, DEPARTMENT_NAME)


}
