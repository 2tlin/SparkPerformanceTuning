package SberCyberSecurity

import SberCyberSecurity.DataSourceClickHouse.{chProps, driver, password, readTableFromCH, spark, url, user}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.yandex.clickhouse.domain.ClickHouseFormat
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}

import java.io.File
import java.sql.Connection
import java.util.Properties

object DataSourceClickHouseJDBC {
  val spark = SparkSession.builder()
    .appName("DataSource ClickHouse")
    .master("local")
    .getOrCreate()


  // set connection options
  val properties = new ClickHouseProperties();


  val url = s"jdbc:clickhouse://localhost:8123/default" // DO NOT USE $dbName - just string
  val driver = "ru.yandex.clickhouse.ClickHouseDriver"

  val filePath = "src/main/resources/data/clickhouse/ch.csv"

  val user = "Dima"
  val password = "191265"

  val dbName = "default"
  val schemaName1 = "default"
  val schemaName2 = "ch1"


  properties.setUser(user)
  properties.setPassword(password)

  val chProps = new Properties()

  chProps.put("driver", driver)
  chProps.put("user", user)
  chProps.put("password", password)

  def createTable2CH(properties: ClickHouseProperties, tableName: String): Unit = {

    var connection : Connection = null

    try {
      val dataSource: ClickHouseDataSource = new ClickHouseDataSource(url, properties)
      connection = dataSource.getConnection()
      val stmt = connection.createStatement()
      val sql =
        s"""
           |create table if not exists default.${tableName}  (
           |    `name` String,
           |    `age`  Int32)
           |ENGINE = MergeTree() ORDER BY `name` SETTINGS index_granularity = 8192;
           |""".stripMargin
      stmt.execute(sql)
    } finally {
      if(connection != null)
        connection.close()
    }
  }

  def writeFile2CH(filePath: String, tableName: String): Unit = {
    var connection: ClickHouseConnection = null

    try {
      val dataSource: ClickHouseDataSource = new ClickHouseDataSource(url, properties)
      connection = dataSource.getConnection()
      val stmt: ClickHouseStatement = connection.createStatement()

      stmt
        .write() // Write API entrypoint
        .table(s"$dbName.$tableName") // where to write data
        .option("format_csv_delimiter", ";") // specific param
        .data(new File(filePath), ClickHouseFormat.CSV) // specify input
        .send();
    } finally {
      if (connection != null)
        connection.close()
    }
  }

  def writeDF2CH(df: DataFrame, tableName: String): Unit = {

    val createTableColumnTypes = "id VARCHAR(1024), Surname VARCHAR(1024), OrderSum VARCHAR(1024)"
    val createTableOptions = s"ENGINE = MergeTree() ORDER BY (id);"

    val repartionedData = df.repartition(8)

    repartionedData
      .write
      .option("createTableColumnTypes", createTableColumnTypes)
      .option("createTableOptions", createTableOptions)
      .mode(SaveMode.Append)
      .jdbc(url, tableName, chProps)
  }

  def writeDF2Table(tableDF: DataFrame, tableName: String, format: String): Unit = {
    tableDF.createOrReplaceTempView(tableName)
    tableDF.write
      .format(format)
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  // read from CH as DFs
  def readTableFromCH(tableName: String): DataFrame = spark
    .read
    .jdbc(url, tableName, chProps)

  def readTable(tableName: String, customSchema: String = ""): DataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("customSchema", customSchema)
    .option("stringtype", "unspecified")
    .option("user", user)
    .option("password", password)
    .option("dbtable", tableName)
    .load()

  def readTableInnerQuery(query: String): DataFrame = spark
    .read
    .jdbc(url, query, chProps)

  def main(args: Array[String]): Unit = {
//    createTable2CH(properties, "Users")
//    writeFile2CH("spark-warehouse/persons2", "Persons2")

    // ttps://clickhouse.com/docs/ru/getting-started/example-datasets/menus/
    // use CH in docker from Docker HUB
    val tableMenu = "menu"
    val tableDish = "dish"
    val tableMenuItem = "menu_item"

    // Spark does not read ClickHouse UUID type.
    // Need to create CH view to cast UUID type in origin table
    val tableMenuPage = "uuid_menu_page_view"
    val tableMenuPage5 = "menu_page5"

    // запись в MATERIALIZED VIEW срабатывает как триггер только при появлении новых записей
    val databaseDF = spark.sql("show databases") // returns DB as a DF
    databaseDF.show()

    spark.sql("use default") // every sub sequent select will be related to default DB

  // Creating CH view to cast UUIT type in origin table ??? - not work from Spark ???

//    spark.sql(
//      """
//        |CREATE VIEW uuid_menu_page_view
//        |AS
//        |	SELECT
//        |		id AS id,
//        |		menu_id AS menu_id,
//        |		page_number AS page_number,
//        |		image_id AS image_id,
//        |		full_height AS full_height,
//        |		full_width AS full_width,
//        |		CAST (uuid AS CHAR(15)) AS uuid
//        |	FROM default.menu_page
//        |""".stripMargin)

    val query = s"(SELECT * FROM $schemaName1.$tableDish WHERE first_appeared >= 1975 AND last_appeared <= 1980 LIMIT 5) e" // $schemaName2.$tableName"

    val query2 = s"""(
      |  select *
      |  from $schemaName1.$tableDish e
      |    where first_appeared >= 1975 AND last_appeared = 1980 and name = 'Te (Tea)'
      |    limit 5
      |)""".stripMargin

    val customSchemaMenuPage = "id DECIMAL(38, 0), image_id STRING, uuid CHAR(15)" // CAST (uuid AS CHAR(15)) -- not work

    val menuDF = readTable(tableMenu)
    val dishDF = readTable(tableDish)
    val menuItemDF = readTable(tableMenuItem)
    val menuPageDF = readTable(tableMenuPage)
//    val menuPage5DF = readTable(tableMenuPage5, customSchemaMenuPage) // not work
    val queriedDishDF = readTableInnerQuery(query2)

    menuDF.show()
    dishDF.show()
    menuItemDF.show()
    menuPageDF.show()
    queriedDishDF.show()

//    Thread.sleep(10000000)
  }
}
