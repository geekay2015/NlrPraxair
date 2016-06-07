import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


object NlrPraxairCR080025 {

  // Define the application Name
  val AppName: String = "NlrPraxairCR080025"

  // Set the logging level
  Logger.getLogger("org.apache").setLevel(Level.ERROR)

  // Main Method
  def main (args: Array[String]): Unit = {

    // Define the arguments
    val inputFile: String = "file:///Users/gangadharkadam/myapps/NlrPraxair/src/main/resources/NLR_Praxair›2008›3QTR2008›Coater_4›CR080025.csv"
    //val outputFile: String = "hdfs://localhost:9000/user/hive/warehouse/praxair"

    // create the spark configuration and spark context
    println("Initializing the spark context...")

    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster("local")
      .set("spark.speculation", "false")

    val sc = new SparkContext(conf)

    // Define the SQL context
    val sqlContext = new HiveContext(sc)

    // Define the schema
    val praxair4crSchema = StructType(
      Array
    (
      StructField("campaign_number", IntegerType, nullable = true),
      StructField("run_number", IntegerType, nullable = true),
      StructField("carrier_number", IntegerType, nullable = true),
      StructField("gun_recipe", StringType, nullable = true),
      StructField("operator_loading_station", IntegerType, nullable = true),
      StructField("operator_workstation", IntegerType, nullable = true),
      StructField("feeder_number", IntegerType, nullable = true),
      StructField("thermocouple_zone_1", IntegerType, nullable = true),
      StructField("thermocouple_zone_2", IntegerType, nullable = true),
      StructField("thermocouple_zone_3", IntegerType, nullable = true),
      StructField("hch_tc_1_temperature", DoubleType, nullable = true),
      StructField("hch_tc_2_temperature", DoubleType, nullable = true),
      StructField("hch_tc_3_temperature", DoubleType, nullable = true),
      StructField("hch_tc_4_temperature", DoubleType, nullable = true),
      StructField("hch_tc_5_temperature", DoubleType, nullable = true),
      StructField("chamber_pressure_hch", DoubleType, nullable = true),
      StructField("ar_gas_flow", DoubleType, nullable = true),
      StructField("coating_start", StringType, nullable = true),
      StructField("coating_finished", StringType, nullable = true),
      StructField("duration_string", StringType, nullable = true),
      StructField("cch_tc_1_temperature", DoubleType, nullable = true),
      StructField("cch_tc_2_temperature", DoubleType, nullable = true),
      StructField("cch_tc_3_temperature", DoubleType, nullable = true),
      StructField("cch_tc_4_temperature", DoubleType, nullable = true),
      StructField("cch_tc_5_temperature", DoubleType, nullable = true),
      StructField("topplate_position", DoubleType, nullable = true),
      StructField("chamber_pressure_cch", DoubleType, nullable = true),
      StructField("ar_gas_flow_cch", DoubleType, nullable = true),
      StructField("o2_gas_flow_cch", DoubleType, nullable = true),
      StructField("emission_current_gun1", DoubleType, nullable = true),
      StructField("emission_current_gun2", DoubleType, nullable = true),
      StructField("power_gun1", DoubleType, nullable = true),
      StructField("power_gun2", DoubleType, nullable = true),
      StructField("pressure_stage_gun_1", DoubleType, nullable = true),
      StructField("pressure_stage_gun_2", DoubleType, nullable = true),
      StructField("start_feed_rate_setpoint_ingot_1", DoubleType, nullable = true),
      StructField("start_feed_rate_setpoint_ingot_2", DoubleType, nullable = true),
      StructField("end_feed_rate_setpoint_ingot_1", DoubleType, nullable = true),
      StructField("end_feed_rate_setpoint_ingot_2", DoubleType, nullable = true),
      StructField("used_length_ingot_1", DoubleType, nullable = true),
      StructField("used_length_ingot_2", DoubleType, nullable = true),
      StructField("used_slot_number_ingot_1", DoubleType, nullable = true),
      StructField("used_slot_number_ingot_2", DoubleType, nullable = true),
      StructField("used_ingot_1_at_start_of_run", StringType, nullable = true),
      StructField("used_ingot_2_at_start_of_run", StringType, nullable = true),
      StructField("used_ingot_1_at_end_of_run", StringType, nullable = true),
      StructField("used_ingot_2_at_end_of_run", StringType, nullable = true)

    )
    )

    // Load and parse the csv file into spark data frame
    val inputRDD = sqlContext.read
      // Define the file format to read
      .format("com.databricks.spark.csv")

      // get the custom schema
      .schema(praxair4crSchema)

      // Use the first line as header
      .option("header","true")

      // Infer the schema
      .option("inferschema", "true")

      // Load the file
      .load(inputFile)

    // Print the schema
    inputRDD.printSchema()

    // Write the DataFrame data to HDFS location in csv format
    inputRDD.write.mode(SaveMode.Overwrite).parquet("PraxairCoater4CR.parquet")

    // drop the external table if it exist
    sqlContext.sql("drop table praxaircoater4cr").show()

    // Create a Spark Hive Context Over the spark Context
    sqlContext.createExternalTable("praxaircoater4cr","/user/gangadharkadam/PraxairCoater4CR.parquet","parquet")

    // Query the hive
    sqlContext.sql("show tables").show()
    sqlContext.sql("describe praxaircoater4cr").show()
    sqlContext.sql("select * from  praxaircoater4cr limit 25").show()


    //stop the context
    println("Stopping Spark Context..")
    sc.stop()

  }

}
