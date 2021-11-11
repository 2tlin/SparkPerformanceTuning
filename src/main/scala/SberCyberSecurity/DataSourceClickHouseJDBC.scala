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
  val tableName = "dish"

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

  def readTable(tableName: String): DataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
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

//    val dishesDF = spark.sql(s"select * from $schemaName2.$tableName") // NOT work. Need to add DATABASE
//    dishesDF.show()

//    val personsdDF1 = readTableFromCH(s"$schemaName1.$tableName") // work
//    val personsdDF2 = readTable(s"$schemaName2.$tableName") // work
//    personsdDF1.show()

    val query = s"(SELECT * FROM $schemaName1.$tableName WHERE first_appeared >= 1975 AND last_appeared <= 1980) e" // $schemaName2.$tableName"

    val dishesDF = readTableInnerQuery(query)
    dishesDF.show()


  }
}
