package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {
  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  @Test def `basic types are there`: Unit = {
    val types = try {
      val langIndex: LangIndex = 3
      val highScore: HighScore = 99
      val qId:       QID       = 101
      true
    } catch {
      case _: Throwable => false
    }
    assert(types, "Can't create Posting objs")
  }

  val que:    Question  =
    Posting(postingType = 1,
      id                = 10,
      acceptedAnswer    = Some(2),
      parentId          = None,
      score             = 19,
      tags              = Some("scala is fun and oooooo"))
  val answer: Answer    =
    Posting(postingType = 2,
      id                = 11,
      acceptedAnswer    = None,
      parentId          = Some(10),
      score             = 99,
      tags              = Some("scala and haskell are fun"))

@Test def `question is a question` = { assert(que.postingType == 1) }
@Test def `answer is an answer` =    { assert(answer.postingType == 2) }


  /**
    * Creates a truncated string representation of a list, adding ", ...)" if there
    * are too many elements to show
    * @param l The list to preview
    * @param n The number of elements to cut it at
    * @return A preview of the list, containing at most n elements.
    */
  def previewList[A](l: List[A], n: Int = 10): String =
    if (l.length <= n) l.toString
    else l.take(n).toString.dropRight(1) + ", ...)"

  /**
    * Asserts that all the elements in a given list and an expected list are the same,
    * regardless of order. For a prettier output, given and expected should be sorted
    * with the same ordering.
    * @param given The actual list
    * @param expected The expected list
    * @tparam A Type of the list elements
    */
  def assertSameElements[A](given: List[A], expected: List[A]): Unit = {
    val givenSet = given.toSet
    val expectedSet = expected.toSet

    val unexpected = givenSet -- expectedSet
    val missing = expectedSet -- givenSet

    val noUnexpectedElements = unexpected.isEmpty
    val noMissingElements = missing.isEmpty

    val noMatchString =
      s"""
         |Expected: ${previewList(expected)}
         |Actual:   ${previewList(given, 55)}""".stripMargin

    assert(noUnexpectedElements,
      s"""|$noMatchString
          |The given collection contains some unexpected elements: ${previewList(unexpected.toList, 55)}""".stripMargin)

    assert(noMissingElements,
      s"""|$noMatchString
          |The given collection is missing some expected elements: ${previewList(missing.toList, 55)}""".stripMargin)
  }

  // Conditions:
  // (1) the language stats contain the same elements
  // (2) they are ordered (and the order doesn't matter if there are several languages with the same count)
  def assertEquivalentAndOrdered(given: List[(String, Int)], expected: List[(String, Int)]): Unit = {
    // (1)
    assertSameElements(given, expected)
    // (2)
    assert(
      !(given zip given.tail).exists({ case ((_, occ1), (_, occ2)) => occ1 < occ2 }),
      "The given elements are not in descending order"
    )
  }


@Test def `sc parse file to gen vectors` = {
    testObject
    import StackOverflow._
    val lines = sc.textFile("src/main/resources/stackoverflow/sample2.csv")
    val vectors = vectorPostings(
      scoredPostings (
        groupedPostings(
          rawPostings(lines) )
      )//.sample(true, 0.9, 0)
    )
    assert ( vectors.takeSample(false,  10).size == 10 )
    val means = kmeans(sampleVectors(vectors), vectors, iter=118, debug=true)
    val results = clusterResults(means, vectors)
    val expected0 = List(("C++",100.0,40,0), ("Objective-C",100.0,43,0), ("MATLAB",100.0,2,0), ("Scala",100.0,5,0), ("Ruby",100.0,33,0), ("PHP",100.0,144,0), ("Haskell",100.0,9,1), ("MATLAB",100.0,12,1), ("Python",100.0,142,1), ("Java",100.0,368,1), ("JavaScript",100.0,333,1), ("CSS",100.0,106,1), ("Groovy",100.0,2,1), ("Clojure",100.0,2,1), ("C#",100.0,239,1), ("C++",100.0,173,2), ("Objective-C",100.0,71,2), ("Scala",100.0,5,2), ("PHP",100.0,253,2), ("Perl",100.0,9,2), ("Groovy",100.0,1,3), ("C#",100.0,211,3), ("JavaScript",100.0,110,4), ("Perl",100.0,4,4), ("Clojure",100.0,4,4), ("Groovy",100.0,1,5), ("Ruby",100.0,21,5), ("Java",100.0,92,6), ("Haskell",100.0,6,7), ("Python",100.0,53,7), ("Scala",100.0,4,8), ("Perl",100.0,4,10), ("CSS",100.0,8,13), ("MATLAB",100.0,1,13), ("C++",100.0,23,14), ("Clojure",100.0,1,19), ("Haskell",100.0,3,22), ("Objective-C",100.0,7,26), ("Python",100.0,9,31), ("PHP",100.0,7,41), ("Ruby",100.0,1,52), ("C#",100.0,6,87), ("JavaScript",100.0,7,135), ("CSS",100.0,1,256))
    //  after correcting median defn:
    val expected  = List(("C++",100.0,40,0), ("Objective-C",100.0,43,0), ("MATLAB",100.0,2,0), ("Scala",100.0,5,0), ("Ruby",100.0,33,0), ("Groovy",100.0,2,0), ("Clojure",100.0,2,0), ("PHP",100.0,144,0), ("Haskell",100.0,9,1), ("MATLAB",100.0,12,1), ("Python",100.0,142,1), ("Java",100.0,368,1), ("JavaScript",100.0,333,1), ("CSS",100.0,106,1), ("C#",100.0,239,1), ("C++",100.0,173,2), ("Objective-C",100.0,71,2), ("Scala",100.0,5,2), ("PHP",100.0,253,2), ("Perl",100.0,9,2), ("Perl",100.0,4,3), ("Clojure",100.0,4,3), ("Groovy",100.0,1,3), ("C#",100.0,211,3), ("JavaScript",100.0,110,4), ("Groovy",100.0,1,5), ("Ruby",100.0,21,5), ("Scala",100.0,4,6), ("Haskell",100.0,6,6), ("Java",100.0,92,6), ("Python",100.0,53,7), ("Perl",100.0,4,9), ("CSS",100.0,8,12), ("MATLAB",100.0,1,13), ("C++",100.0,23,14), ("Clojure",100.0,1,19), ("Haskell",100.0,3,22), ("Objective-C",100.0,7,26), ("Python",100.0,9,31), ("PHP",100.0,7,41), ("Ruby",100.0,1,52), ("C#",100.0,6,84), ("JavaScript",100.0,7,135), ("CSS",100.0,1,256))
    printResults(results)
    sc.stop()
    assertSameElements(results.toList, expected)
  }

@Test def `test the tests` = { assert(1 + 1 == 2) }

  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
