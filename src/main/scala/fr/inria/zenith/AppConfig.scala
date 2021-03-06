package fr.inria.zenith

import java.io.File
import java.util.concurrent.TimeUnit

import fr.inria.zenith.TSToDBMulti.TSWithStats
import org.apache.commons.cli.CommandLine

import scala.collection.mutable
import scala.math.{Numeric, pow, sqrt}


case class AppConfig (cmd: CommandLine){




  val tsFilePath : String = cmd.getOptionValue("tsFilePath", "")
  val tsNum : Int = cmd.getOptionValue("tsNum").toInt
  val sizeSketches : Int = cmd.getOptionValue("sizeSketches", "30").toInt
  val gridDimension : Int = cmd.getOptionValue("gridDimension", "2").toInt
  val gridSize : Int = cmd.getOptionValue("gridSize", "8").toInt
  val cellSize : Int = cmd.getOptionValue("cellSize", "2").toInt

  val numPart : Int = cmd.getOptionValue("numPart", "128").toInt

  val gridsResPath : String = cmd.getOptionValue("gridsResPath", "ts_gridsdb" + "_" + tsNum + "_" + sizeSketches + "_" + gridDimension + "_" + gridSize)

  val queryFilePath : String = cmd.getOptionValue("queryFilePath", "")
  val gridConstruction : Boolean = cmd.getOptionValue("gridConstruction", "false").toBoolean
  val saveResult : Boolean = cmd.getOptionValue("saveResult", "true").toBoolean
  val candThresh : Float = cmd.getOptionValue("candThresh", "0").toFloat
  val topCand : Int = cmd.getOptionValue("topCand", "10").toInt

  val sampleSize : Float =  cmd.getOptionValue("sampleSize", "0.1").toFloat


  val numGroups = sizeSketches / gridDimension

  val firstCol : Integer = cmd.getOptionValue("firstCol", "1").toInt

  /** PATHs **/

 val inputFile = new File(tsFilePath)
 val queryFile = new File(queryFilePath)

  val resPath : String = "/tmp/" + "sketch_" + inputFile.getName +"_" + gridSize.toString + "_" + queryFile.getName + "_" + candThresh.toString + "_"

  val kryoClasses =   Array (
    classOf[Array[Float]],
    classOf[Array[String]],
    classOf[Array[Array[Float]]],
    classOf[Array[Array[Int]]],
    classOf[scala.collection.mutable.ArraySeq[Float]],
    classOf[Array[Object]],
    classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
    classOf[scala.collection.immutable.Vector[Float]],
    Class.forName("[Lorg.apache.spark.util.collection.CompactBuffer;"),
    Class.forName("[Lscala.reflect.ClassTag$$anon$1;"),
    Class.forName("[I"),
    Class.forName("[B"),
    Class.forName("java.util.HashMap"),
    Class.forName("scala.collection.mutable.WrappedArray$ofRef"),
    Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
    Class.forName("scala.collection.immutable.Set$EmptySet$")
  )



  // Product of vector and matrix
  def mult[A](a: Array[A], b: Array[Array[A]])(implicit n: Numeric[A]) = {
    import n._
    for (col <- b)
      yield
        a zip col map Function.tupled(_*_) reduceLeft (_+_)
  }

  //Random vectors Array[Array[Floats]] generator where a  = size of Sketches, b = size of Random vector
  def ranD(a: Int, b: Int)= {
    (for (j<-0 until a) yield
      (for (i <- 0 until b) yield (scala.util.Random.nextInt(2) * 2 - 1).toFloat).toArray).toArray
  }

  def getMinSec (millis : Long) =
    "%d min %d sec".format(TimeUnit.MILLISECONDS.toMinutes(millis), TimeUnit.MILLISECONDS.toSeconds(millis) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)))

  def stats(ts: Array[Float]) : (Float, Float) = {
    val mean = ts.sum / ts.length
    val stdev = sqrt( ts.map(x => x * x).sum / ts.length - mean * mean ).toFloat

    (mean, stdev)
  }

  def normalize(tsWithStats: TSWithStats) : Array[Float] =
    tsWithStats._1.map( x => if (tsWithStats._2._2 > 0.000001) (x - tsWithStats._2._1) / tsWithStats._2._2 else 0.toFloat )

  def distance(xs: TSWithStats, ys: TSWithStats) : Float =
    sqrt((normalize(xs) zip normalize(ys)).map { case (x, y) => pow(y - x, 2)}.sum).toFloat


  def mergeDistances(xs: Array[(Long, Array[Float], Float)], ys: Array[(Long, Array[Float], Float)]) = {
    var rs = new mutable.ListBuffer[(Long, Array[Float], Float)]()
    var i = 0

    for (x <- xs) {
      while (i < ys.length && x._3 > ys(i)._3) {
        rs += ys(i)
        i += 1
      }

      rs += x
    }

    if (i < ys.length)
      rs ++= ys.slice(i, ys.length)

    rs.take(topCand).toArray
  }

 def tsToSketch(tsWithStats: TSWithStats, RandMxBroad: Array[Array[Float]]) : Array[Array[Int]] = {
    val ts = normalize(tsWithStats)
    mult(ts, RandMxBroad).map(v => (v / cellSize).toInt - (if (v < 0) 1 else 0)).sliding(gridDimension, gridDimension).map(_.toArray).toArray

  }

  def tsProgrSketch(tsWithStats: TSWithStats, RandMxBroad: Array[Array[Float]], breakpoints: Array[Array[Float]]) : Array[Array[Int]] = {
    val ts = normalize(tsWithStats)
    mult(ts, RandMxBroad).zipWithIndex.map(v => breakpoints(v._2).indexWhere(v._1 <= _)).map(v => if(v == -1) breakpoints(0).length-1 else v).sliding(gridDimension, gridDimension).map(_.toArray).toArray

  }

}
