package SberCyberSecurity

import org.apache.spark.sql.SparkSession

object DataSourcePostgerSQL {
  val spark = SparkSession.builder()
    .appName("Catalyst Demo")
    .master("local")
    .getOrCreate()

  // reading from remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  def main(args: Array[String]): Unit = {

  }
}
