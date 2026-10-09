package jobs

import core.SparkJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object PruningPartition extends SparkJob {
  def run(implicit spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.adaptive.enabled", false)

    import spark.implicits._
    import spark.sqlContext

    // Big DataFrame
    val df_transactions = Seq(
      (1, "US", 100),
      (2, "IN", 200),
      (3, "UK", 150),
      (4, "US", 80)
    ).toDF("id", "country_code", "amount")

    // Small DataFrame
    val df_countries = Seq(
      ("US", "United States"),
      ("IN", "India"),
      ("UK", "United Kingdom")
    ).toDF("country_code", "country_name")

    df_transactions.show()
    df_transactions.show()

    val joined = df_transactions.join(broadcast(df_countries), "country_code")

    joined.show()
    joined.explain()
  }
}
