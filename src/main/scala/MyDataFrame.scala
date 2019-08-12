object MyDataFrame extends App with Context {
val dfTags = sparkSession
  .read
  .option("Header","true")
  .option("inferschema","true")
  .csv("src/resources/question_tags_10K.csv")
  .toDF("id","tag")

  dfTags.show(10)

}
