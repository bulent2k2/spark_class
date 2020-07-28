// To add to Example.scala sample2

// Mixing and matching RDD/functional API with SQL coolness/optimization

// Create dataset Dataset[Listing], many ways to do that
// 1)
val listingsDS = listingsDF.toDS // requires import spark.implicits._
// 2) 
// val myDS = spark.read.json("people.json").as[Person]
// 3) 
// myRDD.toDS // requires spark.impl* as in 1)
// 4) 
List("yippie", "yay", "hooray").toDS  // also requires spark implicits as in 1)

// and then:
listingsDS.groupByKey(l => l.zip)     // zip is case class named arg -- this is RDD/functional API
  .agg(avg($"price").as[Double])      // these are SQL/DataFrame operators
// note: ds.agg is operating on org.apache.spark.sql.TypedColumn[...], but avg(..) returns a Column. Hence .as[] api.
