package timeusage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.{max,min,count}

object Samples {
  @transient lazy val conf: SparkConf    = new SparkConf().setMaster("local").setAppName("Scratch1")
  @transient lazy val sc  : SparkContext = new SparkContext(conf)

  val sqlContext= new org.apache.spark.sql.SQLContext(sc) // without these two, toDF wouldn't work..
  import sqlContext.implicits._

  def stop() = sc.stop()

  /*
   * Example 1
   */
  case class Employee(id: Int, fname: String, lname: String, age: Int, city: String)
  val sample = List(
    Employee(1, "Baba", "Basar",  49, "Ankara"),
    Employee(2, "Anne", "Keskin", 49, "Izmir"),
    Employee(3, "Evren", "Basar", 17, "Atlanta"),
    Employee(4, "Kayra", "Basar", 13, "Atlanta"),
    Employee(5, "Baray", "Basar",  8, "Atlanta")
  )
  def sample1() = {
    val employeeDF = sc.parallelize(sample).toDF()
    employeeDF.show
    val atlTeam = employeeDF
      .select("id", "lname", "fname")
      .where("city == 'Atlanta'") // === .filter(...)
      .orderBy("id")
    // Filter gives more power (but harder to optimize by spark compiler/code?):
    atlTeam.filter(($"age" < 15) && ($"city" === "Atlanta")).show
  }

  /*
   * Example 2
   */
  case class Listing(street: String, zip: Int ,price: Int)
  val sampleDB = List(
    Listing("Echo Dr",   30345, 600000),
    Listing("Echo Dr 2", 30345, 700000),
    Listing("Echo Dr 3", 30345, 500000),
    Listing("Oak Grove Mdws",   30033, 1100000),
    Listing("Oak Grove Mdws 2", 30033, 1000000),
    Listing("Oak Grove Mdws 3", 30033,  900000)
  )
  def sample2() = {
    val listings = sc.parallelize(sampleDB).toDF()
    listings.show
    listings.groupBy($"zip").max("price").show
    listings.groupBy($"zip").min("price").show
  }

  /*
   * Example 3
   */
  case class Post(author: Int, forum: String, likes: Int, date: String)
  val sampleDB3 = List(
    Post(101, "fun", 5, "Jan 24"),    Post(101, "filo", 5, "Jan 24"),    Post(101, "fun", 11, "Apr 27"),
    Post(102, "filo", 11, "Apr 27"),  Post(102, "art", 11, "Apr 27"),    Post(102, "filo", 3, "Mar 23"),
    Post(103, "art", 15, "Jul 26"),   Post(103, "art", 15, "Jul 26"),    Post(103, "art", 15, "Jul 26"),    Post(103, "art", 15, "Jul 26")
  )

  def sample3() = {
    val posts = sc.parallelize(sampleDB3).toDF()
    posts.show
    posts
      .groupBy($"author", $"forum")
      .agg(count($"author"))  // new DF with columns: author/forum/count(author)
      .orderBy($"forum",$"count(author)".desc).show
  }
}

