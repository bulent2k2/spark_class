package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  val mdebug = // my tiny test case to dev/test/debug
    false

  /** Main function */
  def main(args: Array[String]): Unit = {

    // sample is a tiny test case..
    val data = if (mdebug) "sample2" else "stackoverflow"
    //val data    = "stackoverflow"
    //val data    = "sample"
    val lines   = sc.textFile("src/main/resources/stackoverflow/" + data +  ".csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    if (false && mdebug) {
      grouped.collect.foreach(println)
      scored.collect.foreach(println)
      vectors.collect.foreach(println)
      println(s"vectors has: ${vectors.count()} elements")
    }
    val expectedCount = if (mdebug) 2576 else 2121822 // 72 for the tiny test case
    // assert(vectors.count() == expectedCount, "Incorrect number of vectors: " + vectors.count())
//    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
    sc.stop()
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(_.postingType == 1).map((post: Question) => (post.id: QID, post))
    val answers   = postings.filter(_.postingType == 2).map((post: Answer)   => post.parentId match {
      case Some(qid) => (qid: QID, post)
      case None      => (-1: QID,   post)  // does a question with id -1 exist?
    }).filter { case (id, _) => id != -1 }
    questions.join(answers).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }
    grouped
      .map{ case (_, pairs) => (pairs.head._1, pairs.map{ case (_,a) => a }) }
      .mapValues(pairs => answerHighScore(pairs.toArray))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored
      .map { case(q, highScore) => {
        val index = firstLangInTag(q.tags, langs) match {
          case Some(i) => i * langSpread
          case None => -1
        }
        (index, highScore) } }
      .filter { case (index, _) => index != -1 }
      .persist // BBX: Note! without this, the run time explodes!
      // .cache   -- slower for my single core laptop
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
  //    val newMeans = means.clone() // you need to compute newMeans
    // println(s"means has size ${means.size}:")
    // means.foreach(println)
    // type Cluster = Int // index into the Array for means/newMeans, 0 to 44..
    // type MVector = (LangIndex, HighScore) // a 2D math/physics vector in Cartesian(Z,Z), not Scala Vector!
    // These vals are costly in memory!
    // val closest: RDD[(Cluster, MVector)] = vectors.map(p => (findClosest(p, means), p))
    // val clusters: RDD[(Cluster, Iterable[MVector])] = closest.groupByKey()
    // val newMeansRDD: RDD[(Cluster, MVector)] = clusters.mapValues(iter => averageVectors(iter))
    // val newMeansMap = newMeansRDD.collectAsMap
    val newMeansMap = vectors
      .map(p => (findClosest(p, means), p))
      .groupByKey()
      .mapValues(averageVectors(_))
      .collectAsMap
    // Simpler way to "clone" and override the array (cluster indices to stay invariant) 
    val newMeans = means.indices.map(i=>newMeansMap.getOrElse(i, means(i))).toArray
    /* The longer way to illustratee..
    val newMeans: Array[(Int,Int)] = means.zipWithIndex.map {
      case (pair, index) => newMeansMap.get(index) match {
        case Some(newPair) => newPair
        case None          => pair
      }
    }
     */
    // println(s"newMeans has size ${newMeans.size}:")
    // newMeans.foreach(println)


    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    type MVector = (LangIndex, HighScore) // a 2D math/physics vector in Cartesian(Z,Z), not Scala Vector!

    // return a sorted list of langs by count of questions
    def sortPerLangCount(vs: List[MVector]): List[(LangIndex, Int)] = {
      val ls = for ((langIndex, _) <- vs) yield(langIndex)
      val counts = for (l <- ls.distinct) yield(l, vs.count{ case (l2, _) => l2 == l })
      counts.sortWith((p1, p2) =>  p1._2 > p2._2)
    }

    val median = closestGrouped.mapValues { vsp =>
      val vs = vsp.toList
      // println(s"Num values: ${vs.size}")
      val sorted =  sortPerLangCount(vs)
      val (topLang, topLangCount) = sorted.head
      val langLabel: String   =  // most common language in the cluster
        langs(topLang / langSpread)
      val total = sorted.foldLeft(0)(_ + _._2)
      val langPercent: Double =  // percent of the questions in the most common language
        (100.0 * topLangCount) / total
      if (topLangCount != total) 
        println(s"top lang count = $topLangCount total: $total")
      val clusterSize: Int    = vs.size
      val sortedHighScores = (for ((_, hs) <- vs) yield(hs)).sorted
      val medianScore: Int = {
        val cnt = sortedHighScores.size
        if (cnt % 2 == 1) sortedHighScores(cnt / 2) else (sortedHighScores(cnt / 2) + sortedHighScores(cnt / 2 - 1))/2
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
