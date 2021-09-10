import SberCyberSecurity.DataSourceClickHouse.chProps
import org.apache.spark.sql.{DataFrame, SaveMode}
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}
import ru.yandex.clickhouse.domain.ClickHouseFormat
import ru.yandex.clickhouse.settings.ClickHouseProperties

import java.io.File
import java.sql.Connection

package object SberCyberSecurity {

  case class Persons(Name: String, Surname: String, Age: Int, Occupation: String, id: Int)
  // set connection options
  val properties = new ClickHouseProperties();

  val user = "default"
  val password = "191265"
  val dbName = "default"
  val url = s"jdbc:clickhouse://localhost:8123/$dbName"
  val filePath = "src/main/resources/data/clickhouse/ch.csv"

  properties.setUser(user)
  properties.setPassword(password)

  def createTabletoCH(properties: ClickHouseProperties, tableName: String): Unit = {

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
}
