// scalastyle:off println
package fr.inria.zenith

import org.apache.commons.cli.{BasicParser, CommandLine, Options}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Usage:
  * Grid construction
  * $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val --gridConstruction true  --numPart int_val --tsNum int_val --nodesFile path
	*
  * Index creation
  * $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val  --numPart int_val --tsNum int_val
  *
  * Query processing
  * $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --queryFilePath path --candThresh dec_val --numPart int_val --tsNum int_val
  */
object TSToDBMulti {


  type DataStatsRDD = RDD[(Long, (Array[Float], (Float, Float)))]
  type TSWithStats = (Array[Float], (Float, Float))

  def initOptions () : Options = {
    val options = new Options()

    options.addOption("tsFilePath", true, "Path to the Time Series input file")
    options.addOption("tsNum", true, "Number of input Time Series  [Default: 4] (Optional)")
    options.addOption("sizeSketches", true, "Size of Sketch [Default: 30]")
    options.addOption("gridDimension", true, "Dimension of Grid cell [Default: 2]")
    options.addOption("gridSize", true, "Fraction of Grid  [Default: 8]")
    options.addOption("cellSize", true, "Size of Grid cell [Default: 2]")


    options.addOption("numPart", true, "Number of partitions for parallel data processing [Default: 8]")
    options.addOption("queryFilePath", true, "Path to a given collection of queries")
    options.addOption("candThresh", true, "Given threshold (fraction) to find candidate time series from the grids [Default: 0.6] ")
    options.addOption("gridConstruction", true, "Boolean parameter [Default: false]")

    options.addOption("batchSize", true, "Size of Insert batch to DB [Default: 1000]")
    options.addOption("nodesFile", true, "Path to the list of cluster nodes (hostname ips) [Default: nodes]")

    //optional
    options.addOption("numGroupsMin", true, "")
    options.addOption("gridsResPath", true, "Path to the result of grids construction")
    options.addOption("jdbcUsername", true, "Username to connect DB (Optional)")
    options.addOption("jdbcPassword", true, "Password to connect DB (Optional)")
    options.addOption("jdbcDriver", true, "JDBC driver to RDB (Optional) [Default: PostgreSQL]")
    options.addOption("queryResPath", true, "Path to the result of the query")
    options.addOption("saveResult", true, "Boolean parameter [Default: true]")
    options.addOption("topCand", true, "Number of top candidates to save  [Default: 10]")

    options.addOption("sampleSize",true, "Size of sample to build progressive grid sizes")

    options
  }


  private def readRDD(sc: SparkContext, tsFile: String, config:  AppConfig) : DataStatsRDD = {

   // val firstCol = config.firstCol
   val firstCol = 1

     val distFile = if ( Array("txt", "csv").contains(tsFile.slice(tsFile.length - 3, tsFile.length)) )
      (if (config.numPart == 0) sc.textFile(tsFile) else sc.textFile(tsFile, config.numPart))
        .map( _.split(',') ).map( t => (t(0).toLong, t.slice(firstCol, t.length).map(_.toFloat) ) )
    else
      if (config.numPart == 0) sc.objectFile[(Long, (Array[Float]))](tsFile)
      else sc.objectFile[(Long, (Array[Float]))](tsFile, config.numPart)

    distFile.map( ts => (ts._1, (ts._2, config.stats(ts._2))) )
  }

  private def createBreakpoints (ts: DataStatsRDD, RandMxBroad: Array[Array[Float]], config:  AppConfig) : Array[Array[Float]] = {

    val sampleInput = ts.sample(false,config.sampleSize).map(t => config.normalize(t._2))
    val sampleProject = sampleInput.map(t => config.mult(t, RandMxBroad)).collect.map(_.toArray).transpose

    val segmentSize = sampleProject(0).length / config.gridSize
 //   val numExtraSegments = length % (cellSize)
 //   val sliceBorder = (cellSize - numExtraSegments) * segmentSize

   // sampleProject.map(toBreakpoint)
    sampleProject.map(_.take(segmentSize * config.gridSize)).map(_.sortWith(_ < _).sliding(segmentSize,segmentSize).map(_.last).toArray)

  }
/*
  def toBreakpoint (sk: Array[Float]) : Array[Float] = {
    val s = sk.sortWith(_ < _)
    (s.slice(0, sliceBorder).sliding(segmentSize, segmentSize) ++ s.slice(sliceBorder, s.length).sliding(segmentSize+1, segmentSize+1)).map(_.last).toArray

  }
*/
  private def rddToSketch(ts: DataStatsRDD, RandMxBroad: Array[Array[Float]], config:  AppConfig): RDD[(Long, Array[Array[Int]])] = ts.map(t => ( t._1 , config.tsToSketch(t._2, RandMxBroad)))

  private def rddToProgrSketch(ts: DataStatsRDD, RandMxBroad: Array[Array[Float]], breakpoints: Array[Array[Float]], config:  AppConfig): RDD[(Long, Array[Array[Int]])] = ts.map(t => ( t._1 , config.tsProgrSketch(t._2, RandMxBroad, breakpoints)))



  def main(args: Array[String]): Unit = {


    val options = initOptions()
    val clParser = new BasicParser()
    val cmd: CommandLine = clParser.parse(options, args)


    val config = AppConfig(cmd)

    val conf: SparkConf = new SparkConf().setAppName("Time Series Grid Construction")
 //   conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 //       .set("spark.kryo.registrationRequired", "true")
 //   conf.registerKryoClasses(config.kryoClasses)

    val sc: SparkContext = new SparkContext(conf)

    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val RndMxPath = new Path(config.gridsResPath + "/config/RndMxGrids")
    val urlHostsPath = new Path(config.gridsResPath + "/config/urlListDB")
    val breakpointsPath = new Path(config.gridsResPath + "/config/breakpoints")


    val t0 = System.currentTimeMillis()
    val rdbStorage = new Storage(config, cmd)


    /** Storage Initialization **/

    val urlList = hdfs.exists(urlHostsPath) match {
      case true => sc.objectFile[String](urlHostsPath.toString, config.numPart).collect().toList
      case false =>
        val urlList = rdbStorage.initialize()
        sc.parallelize(urlList, config.numPart).saveAsObjectFile(urlHostsPath.toString)
        urlList
    }


    /** Grid Construction **/
    if (!hdfs.exists(new Path(config.gridsResPath + "/config/RndMxGrids")) || config.gridConstruction) {

      println("Grid Construction stage for input: " + config.tsFilePath)
      println("numPart = "+ config.numPart)

      val distFileWithStats = readRDD(sc, config.tsFilePath, config)
      val sizeTS = distFileWithStats.first()._2._1.length

      val RandMx = hdfs.exists(RndMxPath) match {
        case true => sc.objectFile[Array[Float]](RndMxPath.toString, config.numPart).collect()
        case false => {
          //  val sizeTS = distFileWithStats.first()._2._1.length
          val RandMx = config.ranD(config.sizeSketches, sizeTS)
          sc.parallelize(RandMx,config.numPart).saveAsObjectFile(RndMxPath.toString)
          RandMx
        }
      }
      val RandMxBC = sc.broadcast(RandMx)

      val breakpoints =   hdfs.exists(breakpointsPath) match {
        case true => sc.objectFile[Array[Float]](breakpointsPath.toString, config.numPart).collect()
        case false => {
          val bpts = createBreakpoints(distFileWithStats, RandMxBC.value, config)
          sc.parallelize(bpts,config.numPart).saveAsObjectFile(breakpointsPath.toString)
          bpts
        }
      }

      breakpoints.map(_.mkString("[",",","]")).foreach(println)

      val bptsBC = sc.broadcast(breakpoints)


   //   val inputSketchRDD = rddToSketch(distFileWithStats, RandMxBroad.value, config)
      val inputSketchRDD = rddToProgrSketch(distFileWithStats, RandMxBC.value, bptsBC.value, config)

      inputSketchRDD
        .mapPartitionsWithIndex((index, part) => rdbStorage.insertPartition(urlList(index % config.numPart), part)).collect()


      val t1 = System.currentTimeMillis()
      println("Grids construction for input: "+ config.tsFilePath + " (Elapsed time): " + (t1 - t0)  + " ms (" +  config.getMinSec(t1-t0) + ")")
    }

    /** Index DB **/
    if (!config.gridConstruction && config.candThresh==0) {
      println("Index construction stage")
      val t1 = System.currentTimeMillis()

      rdbStorage.indexingGrids(urlList)

      val t2 = System.currentTimeMillis()
      println("Index construction (Elapsed time): " + (t2 - t1)  + " ms (" +  config.getMinSec(t2-t1) + ")")
    }

    /** Query  Processing **/
    if (config.queryFilePath!= "" && config.candThresh>0) {
      println("Query processing stage")
      val t3 = System.currentTimeMillis()

      val RandMxSavedBC = sc.broadcast(sc.objectFile[Array[Float]](RndMxPath.toString).collect)
      val bptsSavedBC = sc.broadcast(sc.objectFile[Array[Float]](breakpointsPath.toString).collect)
      val urlListFile = sc.objectFile[String](urlHostsPath.toString).collect().toList

      val queryStatsRDD = readRDD(sc, config.queryFilePath, config)

 //    val querySketchRDD = rddToSketch(queryStatsRDD, RandMxGrids, config)
      val querySketchRDD = rddToProgrSketch(queryStatsRDD, RandMxSavedBC.value, bptsSavedBC.value, config)


      val queryGridsBC = sc.broadcast( querySketchRDD.collect() )

      val queryResFlt = sc.parallelize(0 until config.numPart, config.numPart).mapPartitions(index => rdbStorage.queryDB(urlListFile(index.next()), queryGridsBC.value, (config.candThresh * (config.sizeSketches / config.gridDimension)).toInt))


      /** Saving query result to file or to console **/
      if (config.saveResult) {
          val inputStatsRDD = readRDD(sc, config.tsFilePath, config)

          val queryBC = sc.broadcast(queryStatsRDD.map{case (q_id,q_data)  => q_id -> q_data}.collectAsMap())

          val resultRDD = queryResFlt
            .combineByKey(v => Array(v), (xs: Array[Long], v) => xs :+ v, (xs: Array[Long], ys: Array[Long]) => xs ++ ys)
            .join(inputStatsRDD)
            .mapPartitions{ part =>
              var queryMap = new mutable.HashMap[Long, Array[(Long, Array[Float], Float)]]()
              queryBC.value.foreach(q => queryMap += (q._1 -> Array.empty))

              while (part.hasNext) {
                val (t_id, (q_list, t_data)) = part.next()
                q_list.foreach( q_id => queryMap(q_id) = config.mergeDistances(queryMap(q_id), Array((t_id, t_data._1, config.distance(queryBC.value(q_id), t_data)))) )
              }

              queryMap.toIterator
            }
            .reduceByKey(config.mergeDistances)
            .map{ case(q_id, res) => (q_id, queryBC.value(q_id)._1, res)}
            .map{case (q_id, q_data, res) =>((q_id, q_data.mkString("[",",","]")),res.map(c => "((" + c._1 +  "," + c._2.mkString("[",",","]") + ")," + c._3 + ")").mkString("[",",","]"))}


          /**  Storing the candidates, sorted by Euclidean distance, to the text file  **/
          import java.io._
          val savePath = config.resPath + (System.currentTimeMillis() - t3)
          val pw = new PrintWriter(new File(savePath))
          pw.write(resultRDD.map(_.toString).reduce(_ + '\n' + _))
          pw.write("\n")
          pw.close
          println("Result saved to " + savePath)
    //    }
   //     else println ("No candidates found.")
      }
      else {
 /*       breakable {
          for (i <- 0.1 until candThresh by 0.1) {
            val queryResFlt = queryRes.filter(_._2 > (i * (sizeSketches / gridDimension)).toInt)
            println(s"candThresh = $i, candidates = " + queryResFlt.count)
            if (queryResFlt.count <= 0) break
          }
        }
  */    }

      val t5 = System.currentTimeMillis()
      println("QP + Save res  (Elapsed time): " + (t5 - t3) + " ms (" +  config.getMinSec(t5-t3) + ")")
    }
   sc.stop()
  }
}
// scalastyle:on println