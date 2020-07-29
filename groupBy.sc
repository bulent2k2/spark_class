package misc

object groupBy {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val words = List("boy", "girl", "boy", "man", "woman", "man", "girl", "boy")
                                                  //> words  : List[String] = List(boy, girl, boy, man, woman, man, girl, boy)
  val counts = words.groupBy(w=>w).mapValues(_.size)
                                                  //> counts  : scala.collection.immutable.Map[String,Int] = Map(girl -> 2, boy ->
                                                  //|  3, woman -> 1, man -> 2)
  (counts map {case (k,v) => k ++ " " ++ v.toString}) foreach println
                                                  //> girl 2
                                                  //| boy 3
                                                  //| woman 1
                                                  //| man 2

}