class Pouring(capacity: Vector[Int]) { ; // Only 50 lines!! how big is each glass, 0, 1, ...

  type State = Vector[Int]   // how much water there is in each glass
  val initialState = capacity map (_ => 0)  // start with no water (=0) in each glass

  trait Move {
    def change(state: State): State
  }
  case class Empty(glass: Int) extends Move {
    def change(state: State): State = state updated (glass, 0)
  }
  case class Fill(glass: Int) extends Move {
    def change(state: State): State = state updated (glass, capacity(glass))
  }
  case class Pour(from: Int, to: Int) extends Move {
    def change(state: State): State = {
      val amount = state(from) min (capacity(to) - state(to))
      state updated (from, state(from) - amount) updated (to, state(to) + amount)
    }
  }

  val glasses = 0 until capacity.length
  val moves = // all possible moves
    (for (g <- glasses) yield Empty(g)) ++
      (for (g <- glasses) yield Fill(g)) ++
      (for (from <- glasses; to <- glasses if from != to) yield Pour(from, to))

  class Path(history: List[Move], val endState: State) {
    def extend(move: Move) = new Path(move :: history, move change endState)
    override def toString = history.reverse.mkString(" ") + "--> " + endState
  }
  val initialPath = new Path(Nil, initialState)
  def from(paths: Set[Path], explored: Set[State]): Stream[Set[Path]] =
    if (paths.isEmpty) Stream.empty
    else {
      val more = for {
        path <- paths
        next <- moves map path.extend
        if !(explored contains next.endState)
      } yield next
      paths #:: from(more, explored ++ (more map (_.endState)))
    }
  val pathSets = from(Set(initialPath), Set(initialState))
  def solution(target: Int): Stream[Path] = 
    for {
       pathSet <- pathSets
       path <- pathSet
       if path.endState contains target
    } yield path
}
