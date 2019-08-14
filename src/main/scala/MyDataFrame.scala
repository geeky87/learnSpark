object MyDataFrame extends App with Context {
  val dfTags = sparkSession
    .read
    .option("Header", "true")
    .option("inferschema", "true")
    .csv("src/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)
  dfTags.printSchema()

  // Query dataframe: select columns from a dataframe
  dfTags.select("id", "tag").show(10)
  dfTags.filter("tag ='php'").show(10)

  //Chaining the Functions
  println(s"No of of Lines in the Filter : ${dfTags.filter("tag ='php'").count()}")

  // SQL Queries
  dfTags.filter("tag like 's%'").show(10)
  dfTags.filter("tag in ('php')").show(10)
  dfTags.groupBy("tag").count().show(10)
  dfTags.groupBy("tag").count().filter("count > 2").orderBy("tag").show(10)

  // Reading Another .CSV

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferschema", "true")
    .option("dateformat", "yyyy-MM-dd HH:mm:ss")
    .csv("src/resources/questions_10K.csv")
    .toDF("id",  "creation_date",  "closed_date",  "deletion_date",  "score",  "owner_userid",  "answer_count" )

  dfQuestionsCSV.printSchema()

  // Casting to the Datatypes
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )
  dfQuestions.printSchema()
  dfQuestions.show(10)

  // Join Operation on small subset
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()
  dfQuestionsSubset.show()


  dfQuestionsSubset.join(dfTags,"id")
    .select("owner_userid", "tag", "creation_date", "score")
    .show(10)

  // type of join
  dfQuestionsSubset.join(dfTags,Seq("id"),"inner")
    .show(10)

  dfQuestionsSubset.join(dfTags,Seq("id"),"left_outer")
    .show(10)

  dfQuestionsSubset.join(dfTags,Seq("id"),"right_outer")
    .show(10)



}
