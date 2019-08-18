import org.apache.spark.SparkConf;
import org.apache.spark.sql._;
import java.io.File;
import org.apache.spark.sql.{Row, SaveMode, SparkSession};
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION;


val warehouseLocation = "/user/hive/warehouse";

lazy val sparkConf = new SparkConf()
  .setAppName("Capstone Project")
  .setMaster("local[*]")
  .set(CATALOG_IMPLEMENTATION.key, "hive")
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
  .csv("/user/JIGSGB312/assignments/capstone/CampaignData_full_Big.csv")
  .toDF();

CampaignData.printSchema();

CampaignData.createOrReplaceTempView("T_CampaignData");

val overAllCTOR = sparkSession.sql("SELECT sum(case (CLICK_FLG) when 'Y' then 1 else 0 end)/sum(case (OPEN_FLG) when 'Y' then 1 else 0 end) as overAllCTOR FROM T_CampaignData");
overAllCTOR.show();
val byGenderCTOR = sparkSession.sql("select case I1_GNDR_CODE when 'B' then 'Both' when 'F' then 'Female' when 'M' then 'Male' when 'U' then 'Unknown' else 'no match' end,CTOR from (SELECT I1_GNDR_CODE,sum(case (CLICK_FLG) when 'Y' then 1 else 0 end)/sum(case (OPEN_FLG) when 'Y' then 1 else 0 end) as CTOR FROM T_CampaignData group by I1_GNDR_CODE) A");
byGenderCTOR.show();

val timeOfDayCTOR = sparkSession.sql("SELECT substr(mailed_date,15),sum(case (CLICK_FLG) when 'Y' then 1 else 0 end)/sum(case (OPEN_FLG) when 'Y' then 1 else 0 end) as CTOR FROM T_CampaignData group by substr(mailed_date,15)");
timeOfDayCTOR.show();

val monthOfYearCTOR = sparkSession.sql("select case mon when 01 then 'JAN' when 02 then 'FEB' when 03 then 'MAR' when 04 then 'APR' when 05 then 'MAY' when 06 then 'JUN' when 07 then 'JUL' when 08 then 'AUG' when 09 then 'SEP' when 10 then 'OCT' when 11 then 'NOV' when 12 then 'DEC' end as mon,CTOR from  (SELECT substr(mailed_date,12,2) mon,sum(case (CLICK_FLG) when 'Y' then 1 else 0 end)/sum(case (OPEN_FLG) when 'Y' then 1 else 0 end) as CTOR FROM T_CampaignData group by substr(mailed_date,12,2)) A");
monthOfYearCTOR.show(20,false);

val leadsIncomeCTOR = sparkSession.sql("select case income  when 'J' then '<$15,000' when 'K' then '$15,000-$24,999' when 'L' then '$25,000-$34,999' when 'M' then '$35,000-$49,999' when 'N' then '$50,000-$74,999' when 'O' then '$75,000-$99,999' when 'P' then '$100,000-$119,999' when 'Q' then '$120,000-$149,999' when 'R' then '$150,000+' when 'U' then 'Unknown' else 'No match' end income,CTOR from  (SELECT TRW_INCOME_CD_V4 income,sum(case (CLICK_FLG) when 'Y' then 1 else 0 end)/sum(case (OPEN_FLG) when 'Y' then 1 else 0 end) as CTOR FROM T_CampaignData group by TRW_INCOME_CD_V4) A");
leadsIncomeCTOR.show(20,false);

val leadsEthnicityCTOR = sparkSession.sql("select case ASIAN_CD when '00' then 'Unknown' when '05' then 'Chinese' when '24' then 'Japanese' when '25' then 'Korean' when '47' then 'Vietnamese' when '48' then 'Asian' else 'No match' end ASIAN_CD,CTOR from  (SELECT ASIAN_CD,sum(case (CLICK_FLG) when 'Y' then 1 else 0 end)/sum(case (OPEN_FLG) when 'Y' then 1 else 0 end) as CTOR FROM T_CampaignData group by ASIAN_CD) A");
leadsEthnicityCTOR.show(20,false);

val houseHoldCTOR = sparkSession.sql("select case I1_INDIV_HHLD_STATUS_CODE when 'D' then 'Deceased' when 'H' then 'Head' when 'P' then 'Aged parent living home' when 'U' then 'Unknown' when 'W' then 'Spouse' when 'Y' then 'Young adult (Age 19-25)' else 'No match' end I1_INDIV_HHLD_STATUS_CODE,CTOR from (SELECT I1_INDIV_HHLD_STATUS_CODE,sum(case (CLICK_FLG) when 'Y' then 1 else 0 end)/sum(case (OPEN_FLG) when 'Y' then 1 else 0 end) as CTOR FROM T_CampaignData group by I1_INDIV_HHLD_STATUS_CODE) A ");
houseHoldCTOR.show(10,false);

val   table_schema = CampaignData.schema.fields;
var columns : String = "";
table_schema.foreach(f => columns += f.name +" " + f.dataType.typeName + "," );
columns = columns.substring(0,columns.length-1);
sparkSession.sql("create database capstoneacharya");
sparkSession.sql("show databases");
sparkSession.sql("drop table if exists capstoneacharya.TB_CampaignData");
sparkSession.sql("create table capstoneacharya.TB_CampaignData(" + columns + ") row format delimited fields terminated by '|' location '/user/JIGSGB312/assignments/capstone'");
sparkSession.sql("create table TB_CampaignData(acharya String) row format delimited fields terminated by '|' location '/user/JIGSGB312/assignments/capstone'");
//write the dataframe data to the hdfs location for the created Hive table
CampaignData.write
  .format("com.databricks.spark.csv")
  .option("delimiter","|")
  .mode("overwrite")
  .save("/user/JIGSGB312/assignments/capstone");

columns.substring(columns.length-20,columns.length)
CampaignData.write.mode(SaveMode.Overwrite).saveAsTable("TB_CampaignData");






