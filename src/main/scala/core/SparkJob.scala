package core

import java.io.File
import java.net.URI

import scala.io.StdIn
import scala.util.Try

import org.apache.spark.sql.SparkSession

/** Base for runnable Spark scripts.
  *
  * Extend it from an `object` and implement `run`; each object becomes its own
  * entry point, runnable from IntelliJ's gutter icon or `sbt "runMain jobs.Name"`.
  * Pass `--no-wait` to exit right away instead of pausing for the Spark UI.
  */
trait SparkJob {

  /** App name and default job description in the Spark UI. */
  def name: String = getClass.getSimpleName.stripSuffix("$")

  /** Extra Spark settings applied when the session is built. */
  def sparkConf: Map[String, String] = Map.empty

  def logLevel: String = "WARN"

  def run(implicit spark: SparkSession): Unit

  /** Labels the Spark jobs started inside `body` so they're easy to find in the UI. */
  protected def step[T](description: String)(body: => T)(implicit spark: SparkSession): T = {
    spark.sparkContext.setJobDescription(s"$name: $description")
    try body
    finally spark.sparkContext.setJobDescription(name)
  }

  final def main(args: Array[String]): Unit = {
    val eventLogDir = new File("spark-events")
    eventLogDir.mkdirs()

    val builder = SparkSession.builder()
      .appName(name)
      .master("local[*]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", eventLogDir.getAbsoluteFile.toURI.toString)
    sparkConf.foreach { case (k, v) => builder.config(k, v) }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    spark.sparkContext.setJobDescription(name)

    val result = Try(run(spark))
    result.failed.foreach(_.printStackTrace())

    if (!args.contains("--no-wait")) {
      val port = spark.sparkContext.uiWebUrl.map(u => new URI(u).getPort).getOrElse(4040)
      val status = if (result.isSuccess) "finished" else "FAILED"
      println(s"\n[$name] $status - Spark UI at http://localhost:$port")
      println("Press Enter to stop...")
      StdIn.readLine()
    }

    spark.stop()
    if (result.isFailure) sys.exit(1)
  }
}
