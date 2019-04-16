package fr.inria.zenith

import java.sql.Connection

import org.apache.commons.cli.CommandLine
import org.postgresql.util.PSQLException

/**
  * Class to connect external storage (relational database). Provides methods to create connection with DB server, to insert values (grids), to index inserted items and to perform queries on them.
  */
class Storage (config:  AppConfig, cmd: CommandLine)  extends Serializable {

 // private val cmd  = config.cmd

  private val jdbcUsername = cmd.getOptionValue("jdbcUsername", "postgres")
  private val jdbcPassword = cmd.getOptionValue("jdbcPassword", "postgres")
  private val batchSize = cmd.getOptionValue("batchSize", "1000").toInt
  private val nodesFile = cmd.getOptionValue("nodesFile", "nodes")
  private val jdbcDriver = cmd.getOptionValue("jdbcDriver", "org.postgresql.Driver")

  private val columns : List[String] = "id_gr" :: List.tabulate(config.gridDimension)(n => "dim_" + (n + 1).toString) ::: List("id_ts")

  def connDB ( jdbcUrl: String): Connection  = {
    var connection:Connection = null

    try {
      Class.forName(jdbcDriver)
      import java.sql.DriverManager
      connection = DriverManager.getConnection(jdbcUrl,jdbcUsername,jdbcPassword)
    } catch {
      case e : PSQLException => println(e.getMessage)
    }
    if (connection == null) println("connection is null")
    connection
  }

  def initialize() : List[String] =   {
    /********  Create dbs and tables  ********/
    println("Table name: " + config.gridsResPath)
    val createTbl = "create table " + config.gridsResPath + " (" + columns.mkString(" int, ") + " bigint)"
    val dropTbl = "drop table if exists " + config.gridsResPath

    val nodes = scala.io.Source.fromFile(nodesFile).getLines().toList


    val  partitions = (0 until config.numPart).toList
    val urlList = partitions.par.map{ part =>
    val node = nodes(part % nodes.size)
    val host = s"jdbc:postgresql://$node/"
    val dbname = config.gridsResPath + "_part_" + part
    val conn = connDB( host + "postgres")
    val stmt = conn.createStatement
    stmt.executeUpdate("drop database if exists " + dbname)
    try {
        stmt.executeUpdate("create database " + dbname)
      }
    catch {
        case e: PSQLException => println(e.getMessage)
        case _ => println(host + dbname + " created...")
      }

    stmt.close()
    conn.close()

    val url = host + dbname + "?useServerPrepStmts=false&rewriteBatchedStatements=true"
    val connCreate = connDB(url)
    val stmtCreate = connCreate.createStatement()
    stmtCreate.executeUpdate(dropTbl)
    stmtCreate.executeUpdate(createTbl)

    stmtCreate.close()
    connCreate.close()

    url
    }
    urlList.toList
  }

  private val insertSql = "INSERT INTO " + config.gridsResPath + " VALUES (" + "?, " * (config.gridDimension + 1) + "?)"

  private val batches = batchSize * config.numGroups

  def insertPartition(dbUrl: String, part : Iterator[(Long, Array[Array[Int]])]) : Iterator[String] =  {
    val conn = connDB(dbUrl)
    val stmt = conn.prepareStatement(insertSql)

    part.grouped(batches / config.numGroups).foreach { batch =>
      batch.foreach { case (tsId, groups) =>
        groups.zipWithIndex.foreach { case (dims, group) =>
          stmt.setInt(1, group)
          dims.zipWithIndex.foreach(d => stmt.setInt(d._2 + 2, d._1))
          stmt.setLong(config.gridDimension + 2, tsId)
          stmt.addBatch()
        }
      }
      try {
        stmt.executeBatch()
      } catch {
        case e : PSQLException => println(e.getMessage)
      }
    }
    stmt.close()
    conn.close()

    Iterator(dbUrl)
  }



  def indexingGrids( urlList: List[String]) : Unit =  {

    val createIdx = "CREATE INDEX ON " + config.gridsResPath + columns.mkString(" (", ", ", ")")

    val connList = urlList.map(connDB)
    val stmtList  = connList.map(_.createStatement)
    stmtList.par.foreach(_.execute(createIdx))
    stmtList.foreach(_.close)
    connList.foreach(_.close)
  }


  def buildSelectSql(groups: Array[Array[Int]], candThresh: Int) : String = {
    val whereClause = groups.zipWithIndex.map(gdim => gdim._2 +: gdim._1)
      .map(gdim => (columns.take(config.gridDimension + 1) zip gdim).map(colval => colval._1 + "=" + colval._2).mkString("(", " AND ", ")"))
      .mkString(" OR ")

    "SELECT id_ts FROM " + config.gridsResPath + " WHERE " + whereClause + " GROUP BY id_ts HAVING COUNT(*) >= " + candThresh
  }

  def createQueryTable(conn: Connection, queryGrids: Array[(Long, Array[Array[Int]])]) : Unit = {
    val createTbl = "CREATE TEMP TABLE ts_queries (" + columns.mkString(" int, ") + " bigint)"
    val insertSql = "INSERT INTO ts_queries VALUES (" + "?, " * (config.gridDimension + 1) + "?)"
    val createIdx = "CREATE INDEX ON ts_queries" + columns.mkString(" (", ", ", ")")

    {
      val stmt = conn.createStatement()
      stmt.execute(createTbl)
    }

    {
      val stmt = conn.prepareStatement(insertSql)
      queryGrids.foreach { case (tsId, groups) =>
        groups.zipWithIndex.foreach { case (dims, group) =>
          stmt.setInt(1, group)
          dims.zipWithIndex.foreach(d => stmt.setInt(d._2 + 2, d._1))
          stmt.setLong(config.gridDimension + 2, tsId)
          stmt.addBatch()
        }
      }
      try {
        stmt.executeBatch()
      } catch {
        case e : PSQLException => println(e.getMessage)
      }
    }

    {
      val stmt = conn.createStatement()
      stmt.execute(createIdx)
    }
  }


  def queryDB(dbUrl: String, queryGrids: Array[(Long, Array[Array[Int]])], candThresh: Int) : Iterator[(Long, Long)] = {
    queryGrids.sliding(100, 100).flatMap { queryGrids =>

      val conn = connDB(dbUrl)

      createQueryTable(conn, queryGrids)

      val selectSql = "SELECT " + config.gridsResPath + ".id_ts, ts_queries.id_ts AS id_query\n" +
        "FROM " + config.gridsResPath + " JOIN ts_queries ON " + columns.take(config.gridDimension + 1).map(c => config.gridsResPath + "." + c + " = ts_queries." + c).mkString(" AND ") + "\n" +
        "GROUP BY " + config.gridsResPath + ".id_ts, ts_queries.id_ts\n" +
        "HAVING COUNT(*) >= " + candThresh

      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(selectSql)

      val res = Iterator.continually((rs.next(), rs)).takeWhile(_._1).map(res => (res._2.getLong(1), res._2.getLong(2))).toList

      rs.close()
      stmt.close()

      val stmtDrop = conn.createStatement()
      stmtDrop.execute("DROP TABLE ts_queries")

      conn.close()

      res.toIterator
    }
  }


}
