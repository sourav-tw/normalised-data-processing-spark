package org.biki.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{DriverManager, PreparedStatement}
import java.util.Properties

object NormalizedDataProcessing {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Creating connection properties for connecting to postgresql")
    val jdbcUrl = scala.util.Properties.envOrElse("JDBC_URL", "jdbc:postgresql://localhost:5432/bookshop-db")
    val jdbcDriver = scala.util.Properties.envOrElse("JDBC_DRIVER","org.postgresql.postgresql")
    val user = scala.util.Properties.envOrElse("DB_USERNAME","postgres")
    val password = scala.util.Properties.envOrElse("DB_PASSWORD","postgres")

    val connectionProperties = new Properties
    connectionProperties.put("jdbcUrl", jdbcUrl)
    connectionProperties.put("jdbcDriver", jdbcDriver)
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val schema = StructType(List(
      StructField("firstName", StringType),
      StructField("lastName", StringType),
      StructField("id", LongType),
      StructField("address", StructType(Seq(
        StructField("city", StringType),
        StructField("district", StringType),
        StructField("pinCode", StringType)
      ))),
    ))

    logger.info("Create Spark Session")
    val spark = SparkSession.builder()
      .appName("Normalized Data Processing")
      .master("local[3]")
      .getOrCreate()

    logger.info("Read JSON data")
    val employeeDF = spark.read
      .format("json")
      .schema(schema)
      .option("path", "data/")
      .load()

    logger.info("Denormalize complex JSON data")
    val denormalizedEmployeeDF = employeeDF.withColumn("city", expr("address.city"))
      .withColumn("district", expr("address.district"))
      .withColumn("pinCode", expr("address.pinCode"))
      .drop("address")

    logger.info("Get Spark Context")
    val context = spark.sparkContext
    logger.info("Broadcast JDBC connection parameters to each partition/executor")
    val brConnect = context.broadcast(connectionProperties)

    logger.info("Loop through 3 partitions with individual data")
    denormalizedEmployeeDF.coalesce(3).foreachPartition((partition: Iterator[Row]) => {

      logger.info("Get broadcast values")
      val connectionProperties = brConnect.value

      logger.info("Extract values from broadcast object")
      val jdbcUrl = connectionProperties.getProperty("jdbcUrl")
      val user = connectionProperties.getProperty("user")
      val password = connectionProperties.getProperty("password")

      //Class.forName(jdbcDriver)

      val dbConnection = DriverManager.getConnection(jdbcUrl, user, password)
      dbConnection.setAutoCommit(false)

      val db_batchSize = 50
      var prepareStatement: PreparedStatement = null

      logger.info("Loop through each data row in the partition")
      partition.grouped(db_batchSize).foreach(batch => {
        batch.foreach { row => {
          logger.info("Read column values from individual row")
          val employeeIdIdx = row.fieldIndex("id")
          val employeeId = row.getLong(employeeIdIdx)

          val firstNameIdx = row.fieldIndex("firstName")
          val firstName = row.getString(firstNameIdx)

          val lastNameIdx = row.fieldIndex("lastName")
          val lastName = row.getString(lastNameIdx)

          val cityIdx = row.fieldIndex("city")
          val city = row.getString(cityIdx)

          val districtIdx = row.fieldIndex("district")
          val district = row.getString(districtIdx)

          val pinCodeIdx = row.fieldIndex("pinCode")
          val pinCode = row.getString(pinCodeIdx)

          logger.info("Check if employee data exists")
          val sqlString = "SELECT * FROM public.employees WHERE employeeId=?"
          var stmt: PreparedStatement = dbConnection.prepareStatement(sqlString)

          stmt.setLong(1, employeeId)
          val result = stmt.executeQuery()
          var count: Int = 0

          while (result.next()) {
            count = 1
          }

          logger.info("Determine whether the current record is needed to be updated or inserted")
          var dmlOption: String = null
          if (count > 0) {
            dmlOption = "U"
          }
          else {
            dmlOption = "I"
          }

          if (dmlOption == "U") {
            val updateSQLString = "UPDATE public.employees SET firstName=?,lastName=? WHERE employeeId=?"
            prepareStatement = dbConnection.prepareStatement(updateSQLString)

            prepareStatement.setString(1, firstName)
            prepareStatement.setString(2, lastName)
            prepareStatement.setLong(3, employeeId)
          }
          else if (dmlOption == "I") {
            val insertSQLString =
              """
                |WITH INSERTED as (
                |	INSERT INTO public.employeeaddresses(city,district,pinCode) VALUES (?,?,?)
                |	RETURNING ain
                |)
                |INSERT INTO public.employees(firstname,lastname,employeeId,ain)
                |SELECT ?,?,?,ain
                |FROM INSERTED
                |""".stripMargin

            prepareStatement = dbConnection.prepareStatement(insertSQLString)

            prepareStatement.setString(1, city)
            prepareStatement.setString(2, district)
            prepareStatement.setString(3, pinCode)
            prepareStatement.setString(4, firstName)
            prepareStatement.setString(5, lastName)
            prepareStatement.setLong(6, employeeId)
          }
          logger.info("Add row data for batch updates")
          prepareStatement.addBatch()
          stmt.close()
        }
          logger.info("Execute batched records")
          prepareStatement.executeBatch()
        }
      })
      logger.info("Commit records")
      dbConnection.commit()
      logger.info("Close DB connection for each partition")
      dbConnection.close()
    })
      logger.info("Stop Spark Session")
      spark.stop()
  }
}
