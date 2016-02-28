package tupol.sparx.benchmarks

import com.typesafe.config.Config
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
 *
 */
object Benchmark extends SparkRunnable {

  val appName = "serial-killer"

  val requiredParameters = Seq(
    "input.file",
    "path.output"
  )

  def runJob(sc: SparkContext, config: Config) = {

    val serializerDescriptions = Map(
      "com.databricks.spark.csv" ->
        Map[String, String]("header" -> "true"),
      "com.databricks.spark.avro" -> // uncompressed, snappy, deflate
        Map[String, String]("spark.sql.avro.compression.codec" -> "snappy"),
      "parquet" -> // uncompressed, snappy, gzip, lzo
        Map[String, String]("spark.sql.parquet.compression.codec" -> "snappy") //,
    //      "orc" ->
    //        Map[String, String]()
    )

    val sqlContext = new SQLContext(sc)

    val inputFile = config.getString("input.file")
    val outputDir = config.getString("path.output")

    val df0 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(inputFile) //        .limit(200000)
      .scale(200000)

    /**
     * Test the read and write times nd run the benchmark 'runs' times.
     *
     * @param data
     * @param runs
     * @return
     */
    def test(data: DataFrame, runs: Int = 1) = {
      val records = data.count
      (0 until runs).flatMap { runNo =>
        serializerDescriptions.map {
          case (format, options) =>
            val dataRW = createRW(sqlContext, data, format, options)
            val (readMS, writeMS, size) = testReadWriteTimes(sqlContext, dataRW, outputDir)
            BenchmarkResult(runNo, format, records, readMS, writeMS, size)
        }
      }
    }

    /**
     * Test the read and write times and return the averages for each set of parameters.
     *
     * @param data
     * @param runs
     * @return
     */
    def testAverages(data: DataFrame, runs: Int = 1) = {
      val records = data.count

      serializerDescriptions.map {
        case (format, options) =>
          val dataRW = createRW(sqlContext, data, format, options)
          val results = (0 until runs).map(_ => testReadWriteTimes(sqlContext, dataRW, outputDir))
          val (readMSSum, writeMSSum, sizeSum) = results.reduce((r1, r2) => (r1._1 + r2._1, r1._2 + r2._2, r1._3 + r2._3))
          BenchmarkResult(0, format, records, readMSSum / runs, writeMSSum / runs, sizeSum / runs)
      }
    }

    val results = createTestDataFrames(df0, 10, 1).
      flatMap { df =>
        val results = testAverages(df, 3)
        println(f"${"Serializer"}%-32s, ${"Records"}%-12s, ${"Run #"}%-9s, ${"Size [MB]"}%-9s, ${"Read [s]"}%-9s, ${"Write [s]"}%-9s")
        results.foreach { bmr =>
          println(f"${bmr.format}%-32s, ${bmr.records}%12d, ${bmr.runNo}%9d, ${bmr.fileSize.toDouble / 1024 / 1024}%9.2f, ${bmr.readTimeMs / 1000.0}%9.3f, ${bmr.writeTimeMs / 1000.0}%9.3f")
        }
        results
      }

    println(f"${"Serializer"}%-32s, ${"Records"}%-12s, ${"Run #"}%-9s, ${"Size [MB]"}%-9s, ${"Read [s]"}%-9s, ${"Write [s]"}%-9s")
    results.foreach { bmr =>
      println(f"${bmr.format}%-32s, ${bmr.records}%12d, ${bmr.runNo}%9d, ${bmr.fileSize.toDouble / 1024 / 1024}%9.2f, ${bmr.readTimeMs / 1000.0}%9.3f, ${bmr.writeTimeMs / 1000.0}%9.3f")
    }

  }

  type DataRW = (DataFrameReader, DataFrameWriter)

  case class BenchmarkResult(runNo: Int, format: String, records: Long, readTimeMs: Long, writeTimeMs: Long, fileSize: Long)

  def createRW(sqlContext: SQLContext, data: DataFrame, format: String, options: Map[String, String]): DataRW = {
    val reader = options.toSeq.
      foldLeft(sqlContext.read.format(format))((acc, opt) => acc.option(opt._1, opt._2))

    val writer = options.toSeq.
      foldLeft(data.write.format(format))((acc, opt) => acc.option(opt._1, opt._2))

    (reader, writer)
  }

  /**
   * Create a test/sample sequence of DataFrame objects arranged from smaller to larger;
   *
   * @param steps how many times should the data frame be increased
   * @param incrementRatio
   * @return
   */
  def createTestDataFrames(df0: DataFrame, steps: Int, incrementRatio: Double) = {

    require(steps > 0)
    require(incrementRatio > 0)

    (1 to steps).foldLeft(Seq[DataFrame]()) {
      case (stream, s) =>
        df0.scale(s * incrementRatio) +: stream
    }.reverse

  }

  /**
   * Test the read and write speeds of a given DataFrameReader, DataFrameWriter tuple.
   *
   * @param sqlContext
   * @param dataRW a DataFrameReader, DataFrameWriter tuple
   * @param outputPath temporary path where to store the written data
   * @return
   */
  def testReadWriteTimes(sqlContext: SQLContext, dataRW: DataRW, outputPath: String) = {

    import java.util.UUID.randomUUID

    import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }

    val (dfr, dfw) = dataRW

    // Generate a random file name in the given path
    val outputFileName = s"$outputPath/bmt_${randomUUID.toString}"

    val hdfs = FileSystem.get(new HadoopConfiguration())
    val workingPath = new Path(outputFileName)

    removeHdfsFile(outputFileName)

    val writeTime = timeCode(dfw.save(outputFileName))._2
    val readTime = timeCode(dfr.load(outputFileName).count)._2

    val size = hdfs.getContentSummary(workingPath).getLength

    hdfs.delete(workingPath, true)

    (readTime, writeTime, size)

  }

  /**
   * DataFrame decorator adding utility functions for benchmarking.
   *
   * @param data
   */
  implicit class TestDataFrame(val data: DataFrame) {

    /**
     * Reduces or increases the size of a data frame with the given ratio.
     *
     * @param ratio
     * @return
     */
    def scale(ratio: Double): DataFrame = {

      require(ratio > 0)
      require(data.count > 0)

      val i = ratio.toInt
      val r = ratio - i

      val p1 = if (i >= 1) (1 until i).foldLeft(data)((acc, i) => acc unionAll data) else data.limit(0)
      val p2 = data.limit((data.count * r).round.toInt)

      val rez = p1 unionAll p2
      rez
    }

    /**
     * Reduces or increases the size of a data frame to the given number of rows.
     *
     * @param records
     * @return
     */
    def scale(records: Int): DataFrame = {
      require(records > 0)
      require(data.count > 0)
      scale(records.toDouble / data.count)
    }

  }

}
