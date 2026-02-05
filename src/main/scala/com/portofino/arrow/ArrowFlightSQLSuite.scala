package com.portofino.arrow

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import java.sql.DriverManager
import java.time.LocalDate
import java.util.{HashMap, Properties}
import scala.collection.JavaConverters._

/**
 * Arrow Flight SQL 测试程序
 *
 * 测试通过 Arrow Flight SQL JDBC 驱动连接 Doris 并读取数据
 *
 * @author jiangxintong@chinamobile.com 2026/1/28
 */
object ArrowFlightSQLSuite {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Arrow Flight SQL Test")
    println("=" * 60)

    // 直接定义参数，不再依赖 SparkSession
    val host = System.getProperty("arrow.host", "doris-fe")
    val port = System.getProperty("arrow.port", "8070")
    val user = System.getProperty("arrow.user", "root")
    val password = System.getProperty("arrow.password", "")
    val query = System.getProperty("arrow.query", "SELECT * FROM test_db.user_visit")

    try {

      // 第一步：测试直接 JDBC 连接（重新建立连接）
      println("\n" + "=" * 60)
      println("Testing Direct JDBC Connection")
      println("=" * 60)
      try {
        testJdbcConnection(host, port, user, password, query)
        println("\n✓ JDBC测试完成，所有连接已关闭")
      } catch {
        case e: Exception =>
          println(s"✗ JDBC测试失败: ${e.getMessage}")
          e.printStackTrace()
      }

      // 第二步：测试 ADBC 原生 API
      println("\n" + "=" * 60)
      println("Testing ADBC Native API")
      println("=" * 60)
      try {
        testAdbcNativeFlightSQL(host, port, user, password, query)
        println("\n✓ ADBC测试完成，所有连接已关闭")
      } catch {
        case e: Exception =>
          println(s"✗ ADBC测试失败: ${e.getMessage}")
          e.printStackTrace()
      }

      // 第三步：测试 Spark doris 连接器开启 arrow
      val spark = SparkSession
        .builder()
        .appName("ArrowFlightSQLTest")
        .master("local[4]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
      println("\n" + "=" * 60)
      println("Testing Spark JDBC Reader")
      println("=" * 60)
      testSparkJdbcReader(spark)

      println("\n" + "=" * 60)
      println("所有测试完成")
      println("=" * 60)
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
  /**
   * 测试直接 JDBC 连接
   */
  def testJdbcConnection(host: String, port: String, user: String, password: String, query: String): Unit = {
    println("Testing Direct JDBC Connection...")

    // 加载驱动
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")

    // 构建连接 URL
    val url = s"jdbc:arrow-flight-sql://$host:$port" +
      "?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false"
    println(s"URL: $url")

    // 创建连接属性
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)

    // 连接并查询
    val connection = DriverManager.getConnection(url, props)
    try {
      println("✓ Connection established")
      val statement = connection.createStatement()
      try {
        println(s"Executing query: $query")
        val resultSet = statement.executeQuery(query)
        val metadata = resultSet.getMetaData
        val columnCount = metadata.getColumnCount

        // 打印列信息
        println("\nColumns:")
        for (i <- 1 to columnCount) {
          println(s"  ${metadata.getColumnName(i)}: ${metadata.getColumnTypeName(i)}")
        }

        // 打印数据
        println("\nData:")
        var rowCount = 0
        while (resultSet.next() && rowCount < 20) {
          val values = (1 to columnCount).map { i =>
            val value = resultSet.getObject(i)
            if (resultSet.wasNull()) "NULL" else value.toString
          }
          println(s"  ${values.mkString(", ")}")
          rowCount += 1
        }

        println(s"\n✓ Query executed successfully (showing first $rowCount rows)")
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  /**
   * 测试 ADBC 原生 API + Arrow Flight SQL 读取 Doris（核心实现，与版本无关）
   */
  def testAdbcNativeFlightSQL(host: String, port: String, user: String, password: String, query: String): Unit = {
    println("Testing ADBC+Flight SQL")
    // 1. 初始化 Arrow 内存分配器（必须关闭，避免内存泄漏）
    var allocator: BufferAllocator = null
    var database: AdbcDatabase = null
    var connection: AdbcConnection = null
    var statement: AdbcStatement = null
    var queryResult: AdbcStatement.QueryResult = null

    try {
      // 初始化内存分配器（RootAllocator 是所有子分配器的根节点）
      allocator = new RootAllocator(Long.MaxValue)
      println("✓ Arrow BufferAllocator initialized")

      // 2. 构建 ADBC Flight SQL 驱动配置（使用 TypedKey.set() 方法）
      val adbcConfig = new HashMap[String, Object]()
      AdbcDriver.PARAM_URI.set(adbcConfig, s"grpc://$host:$port")
      AdbcDriver.PARAM_USERNAME.set(adbcConfig, user)
      AdbcDriver.PARAM_PASSWORD.set(adbcConfig, password)

      // 3. 加载 ADBC Flight SQL 驱动并创建数据库连接
      val driver = new FlightSqlDriver(allocator)
      database = driver.open(adbcConfig)
      connection = database.connect()
      println(s"✓ ADBC Flight SQL connection established (连接成功)")

      // 4. 创建 Statement 并执行查询
      statement = connection.createStatement()
      statement.setSqlQuery(query)
      println(s"✓ Executing query: $query")
      queryResult = statement.executeQuery()

      // 5. 获取 ArrowReader 来读取结果集
      val reader = queryResult.getReader
      val schemaRoot = reader.getVectorSchemaRoot

      // 6. 打印 Schema 信息
      println("\nArrow Schema:")
      val fields = schemaRoot.getSchema.getFields.asScala
      fields.foreach { field =>
        val fieldType = Types.getMinorTypeForArrowType(field.getType)
        println(s"  ${field.getName}: $fieldType")
      }

      // 7. 读取所有数据（统计总行数，只打印前 20 行）
      println("\nData (top 20 rows):")
      var totalRowCount = 0L
      var printedRows = 0
      while (reader.loadNextBatch()) {
        val batchRowCount = schemaRoot.getRowCount
        for (i <- 0 until batchRowCount) {
          if (printedRows < 20) {
            val rowValues = fields.map { field =>
              val vector = schemaRoot.getVector(field.getName)
              val value = vector.getObject(i)
              if (value == null) "NULL" else vector match {
                case _: DateDayVector => LocalDate.ofEpochDay(value.asInstanceOf[Int].toLong).toString
                case _ => value.toString
              }
            }
            println(s"  ${rowValues. mkString(", ")}")
            printedRows += 1
          }
          totalRowCount += 1
        }
      }
      println(s"  总行数:   $totalRowCount")
    } catch {
      case e: Exception =>
        println(s"❌ ADBC Native API failed: ${e.getMessage}")
        throw e
    } finally {
      // 8. 逆序关闭资源，避免内存泄漏
      if (queryResult != null) queryResult.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
      if (database != null) database.close()
      if (allocator != null) allocator.close()
      println("✓ ADBC resources closed successfully")
    }
  }
  /**
   * 测试 Spark JDBC 读取
   */
  def testSparkJdbcReader(spark: SparkSession): Unit = {
    println("Testing Spark doris arrow ...")
    try {
      val df = spark.read.format("doris")
        .option("doris.table.identifier", "test_db.user_visit")
        .option("doris.fenodes", "doris-fe:8030")
        .option("doris.user", "root")
        .option("doris.password", "")
        .option("doris.read.mode", "arrow")
        .option("doris.read.arrow-flight-sql.port", "8070")
        .load()
      // 第一种 DataFrame API 形式，直接加载数据生成 DataFrame，后续可直接对 DataFrame 进行处理。
      println("=== DataFrame API 读取结果 ===")
      df.printSchema()
      df.show(10) // 展示前 10 行数据
      df.count() // 统计数量

    } catch {
      case e: Exception =>
        println(s"✗ Spark JDBC Reader failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
