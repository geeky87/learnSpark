import java.io.File
import java.util
import java.util.stream.Collectors

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType


object Capstone extends App {
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath;

  lazy val sparkConf = new SparkConf()
    .setAppName("Capstone Project")
    .setMaster("local[*]")
    .set("spark.cores.max", "2");

  lazy val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate();


  val CampaignData = sparkSession
    .read
    .option("header", "true")
    .option("inferschema", "true")
    .option("dateformat", "yyyy-MM-dd HH:mm:ss")
    .csv("/user/JIGSGB312/assignments/capstone/CampaignData_sample.csv")
    .toDF();

  CampaignData.printSchema();

  val   table_schema = CampaignData.schema.fields;
  var columns : String = ""
  table_schema.foreach(f => columns += f.name +" " + f.dataType.typeName + "," )
  columns = columns.substring(0,columns.length-1)
  //table_schema.foreach(f => println(f.name +" " + f.dataType.typeName +","))
  sparkSession.sql("drop table if exists TB_CampaignData");
  sparkSession.sql("create table my_table(" + columns + ") row format delimited fields terminated by '|' location '/user/JIGSGB312/assignments/capstone'");
  //write the dataframe data to the hdfs location for the created Hive table
  CampaignData.write
    .format("com.databricks.spark.csv")
    .option("delimiter","|")
    .mode("overwrite")
    .save("/user/JIGSGB312/assignments/capstone");



}
