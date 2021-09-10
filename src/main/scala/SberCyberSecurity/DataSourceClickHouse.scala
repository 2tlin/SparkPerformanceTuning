package SberCyberSecurity

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.yandex.clickhouse.settings.ClickHouseProperties

import java.util.Properties

object DataSourceClickHouse {
  val spark = SparkSession.builder()
    .appName("DataSource ClickHouse")
    .master("local")
    .getOrCreate()

  val dbName = "default"
  val user = "default"
  val password = "191265"
  val tablePersons = "Persons"
  val tableOrders = "Orders"
  val tablePath = "src/main/resources/data/clickhouse/persons.csv"
  val filePath = "src/main/resources/data/clickhouse/ch.csv"
  val url = s"jdbc:clickhouse://localhost:8123/default"
  val driver = "ru.yandex.clickhouse.ClickHouseDriver"

  // set connection options with Properties
  val chProps = new Properties()

  chProps.put("driver", driver)
  chProps.put("user", user)
  chProps.put("password", password)

//  chProps.put("batchsize","100000")
//  chProps.put("socket_timeout","300000")
//  chProps.put("numPartitions","8")
//  chProps.put("rewriteBatchedStatements","true")

  // set connection options with ClickHouseProperties
  val properties = new ClickHouseProperties();

  properties.setUser(user)
  properties.setPassword(password)


  // Read DFs from CH tables
  val ordersDF: DataFrame = readTableFromCH(tableOrders)
  val personsDF: DataFrame = readTableFromCH(tablePersons)

  // DF transformations

  ordersDF.drop("Name")
  val joinedDF: DataFrame = personsDF.join(ordersDF, "id")
  val selectedJoinedDF: DataFrame = joinedDF
    .select(
      col("id"),
      col("Surname"),
      col("Sum").as("OrderSum")
    )

  // read from CH as DFs
  def readTableFromCH(tableName: String): DataFrame = spark
    .read
    .jdbc(url, tableName, chProps)

  // write DF to CH table
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

  def main(args: Array[String]): Unit = {
    personsDF.show()
    ordersDF.show()
    selectedJoinedDF.show()
    selectedJoinedDF.printSchema()

//    writeDF2Table(personsDF, "persons", "csv") //

    writeDF2CH(selectedJoinedDF, "Joined")
  }
}
