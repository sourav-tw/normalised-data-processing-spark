package org.biki.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.{Properties, UUID}

object NormalizedDataProcessing {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Creating connection properties for connecting to postgresql")
    val connectionProperties: Properties = prepareDBConnectionProperties

    val schema: StructType = InferJSONSchema

    logger.info("Create Spark Session")
    val spark = SparkSession.builder()
      .appName("Normalized Data Processing")
      .master("local[3]")
      .getOrCreate()

    val employeeDF: DataFrame = readEmployeeDataRaw(schema, spark)

    val denormalizedEmployeeDF: DataFrame = denormalizeEmployeeData(employeeDF)

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

      val dbConnection: Connection = DriverManager.getConnection(jdbcUrl, user, password)
      dbConnection.setAutoCommit(false)

      val db_batchSize = 50
      var prepareStatement: PreparedStatement = null

      logger.info("Loop through each data row in the partition")
      partition.grouped(db_batchSize).foreach(batch => {
        batch.foreach { row => {
          val (employeeId: Long, firstName: String, lastName: String, city: String, district: String, pinCode: String) = getDataElements(row)

          var (addressStmt: PreparedStatement, uniqueAddressId: String) = getUniqueAddress(dbConnection,city,district,pinCode)
          if(uniqueAddressId == "")
            uniqueAddressId = null

          logger.info("Check if employee data exists")

          val (stmt: PreparedStatement, count: Int) = checkForExistingEmployee(dbConnection,employeeId)
          logger.info("Determine whether the current record is needed to be updated or inserted")
          var dmlOption: String = null
          if (count > 0) {
            dmlOption = "U"
          }
          else {
            dmlOption = "I"
          }

          if (dmlOption == "U") {
            var updateSQLString: String = null
            if (uniqueAddressId == null){
               updateSQLString = prepareUpdateStatementWithNewAddress
            }
            else {
              updateSQLString = prepareUpdateStatementWithExistingAddress
            }

            prepareStatement = dbConnection.prepareStatement(updateSQLString)

            if (uniqueAddressId == null) {
              prepareStatement.setString(1, city)
              prepareStatement.setString(2, district)
              prepareStatement.setString(3, pinCode)
              prepareStatement.setLong(4, employeeId)
            }
            else {
              prepareStatement.setString(1, firstName)
              prepareStatement.setString(2, lastName)
              prepareStatement.setObject(3, UUID.fromString(uniqueAddressId))
              prepareStatement.setLong(4, employeeId)
            }
          }

          else if (dmlOption == "I") {
            var insertSQLString:String = null

            if (uniqueAddressId == null) {
              insertSQLString = prepareInsertStatementWithNewAddress()
            }
            else {
              insertSQLString = prepareInsertStatementWithExistingAddress()
            }

            prepareStatement = dbConnection.prepareStatement(insertSQLString)

            if (uniqueAddressId == null) {
              prepareStatement.setString(1, city)
              prepareStatement.setString(2, district)
              prepareStatement.setString(3, pinCode)
              prepareStatement.setString(4, firstName)
              prepareStatement.setString(5, lastName)
              prepareStatement.setLong(6, employeeId)
            }
            else{
              prepareStatement.setString(1, firstName)
              prepareStatement.setString(2, lastName)
              prepareStatement.setLong(3, employeeId)
              prepareStatement.setObject(4, UUID.fromString(uniqueAddressId))
            }
          }

          logger.info("Add row data for batch updates")
          prepareStatement.addBatch()
          stmt.close()
          addressStmt.close()
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

  private def getDataElements(row: Row) = {
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
    (employeeId, firstName, lastName, city, district, pinCode)
  }

  private def denormalizeEmployeeData(employeeDF: DataFrame) = {
    logger.info("Denormalize complex JSON data")
    val denormalizedEmployeeDF = employeeDF.withColumn("city", expr("address.city"))
      .withColumn("district", expr("address.district"))
      .withColumn("pinCode", expr("address.pinCode"))
      .drop("address")
    denormalizedEmployeeDF
  }

  private def readEmployeeDataRaw(schema: StructType, spark: SparkSession) = {
    logger.info("Read JSON data")
    val employeeDF = spark.read
      .format("json")
      .schema(schema)
      .option("path", "data/")
      .load()
    employeeDF
  }

  private def InferJSONSchema = {
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
    schema
  }

  private def prepareDBConnectionProperties = {
    val jdbcUrl = scala.util.Properties.envOrElse("JDBC_URL", "jdbc:postgresql://localhost:5432/bookshop-db")
    val jdbcDriver = scala.util.Properties.envOrElse("JDBC_DRIVER", "org.postgresql.postgresql")
    val user = scala.util.Properties.envOrElse("DB_USERNAME", "postgres")
    val password = scala.util.Properties.envOrElse("DB_PASSWORD", "postgres")

    val connectionProperties = new Properties
    connectionProperties.put("jdbcUrl", jdbcUrl)
    connectionProperties.put("jdbcDriver", jdbcDriver)
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties
  }

  private def checkForExistingEmployee(dbConnection:Connection,employeeId:Long) = {
    val sqlString = "SELECT * FROM public.employee_details_view WHERE employeeId=?"
    val stmt: PreparedStatement = dbConnection.prepareStatement(sqlString)

    stmt.setLong(1, employeeId)
    val result = stmt.executeQuery()
    var count: Int = 0

    while (result.next()) {
      count = 1
    }
    (stmt, count)
  }
  private def getUniqueAddress(dbConnection:Connection,city:String,district:String,pinCode:String) = {
    logger.info("Check for existing address")
    val addressSQLString = "SELECT * FROM public.employee_details_view WHERE city=? AND district=? AND pinCode=?"
    val addressStmt: PreparedStatement = dbConnection.prepareStatement(addressSQLString, ResultSet.TYPE_SCROLL_SENSITIVE,
      ResultSet.CONCUR_UPDATABLE)

    addressStmt.setString(1, city)
    addressStmt.setString(2, district)
    addressStmt.setString(3, pinCode)

    val addressResult = addressStmt.executeQuery()
    var uniqueAddressId: String = ""

    if (addressResult.first())
      uniqueAddressId = addressResult.getString("ain")
    (addressStmt, uniqueAddressId)
  }

  private def prepareInsertStatementWithNewAddress(): String = {
    """
      |WITH INSERTED as (
      |	INSERT INTO public.employeeaddresses(city,district,pinCode) VALUES (?,?,?)
      |	RETURNING ain
      |)
      |INSERT INTO public.employees(firstname,lastname,employeeId,ain)
      |SELECT ?,?,?,ain
      |FROM INSERTED
      |""".stripMargin
  }

  private def prepareInsertStatementWithExistingAddress(): String = {
    """
      |INSERT INTO public.employees(firstname,lastname,employeeId,ain)
      |VALUES (?,?,?,?)
      |""".stripMargin
  }

  private def prepareUpdateStatementWithNewAddress: String = {
    """
      |WITH INSERTED as (
      |	INSERT INTO public.employeeaddresses(city,district,pinCode) VALUES (?,?,?)
      |	RETURNING ain
      |)
      |UPDATE public.employees
      |SET ain = (SELECT ain FROM INSERTED)
      |WHERE employeeId=?
      |""".stripMargin
  }

  private def prepareUpdateStatementWithExistingAddress: String = {
    "UPDATE public.employees SET firstName=?,lastName=?,ain=? WHERE employeeId=?"
  }

}
