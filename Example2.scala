// From last video of the last week (week4)

// Spark SQL optimization are nice, but there is no typesafety in DataFrames -- everything is untyped
// Holy Grail is Dataset!
// type DataFrame = Dataset[Row]
// RDD (typed/but hard to optimize) --> Dataset (typed and easier to optimize) --> DataFrame (not typed and easy to optimize)

val averagePrices = averagePricesDF.collect()
// averagePrices: Array[org.apache.spark.sql.Row]

// this throws exception. Try it!
val averagePricesAgain = averagePrices.map {
  row => (row(0).asInstancesOf[String], row(1).asInstancesOf[Int])
}

averagePrices.head.schema.printTreeString()  // from Row API docs
// root
//  |-- zip: integer (nullable = true)
//  |-- avg(price): double (nullable = true)

val averagePricesAgain2 = averagePrices.map {
  row => (row(0).asInstancesOf[Int], row(1).asInstancesOf[Double])
}

