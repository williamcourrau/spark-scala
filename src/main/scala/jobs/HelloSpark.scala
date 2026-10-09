package jobs

import core.SparkJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, spark_partition_id}

object HelloSpark extends SparkJob {
  def run(implicit spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.adaptive.enabled", false)
    spark.conf.set("spark.sql.files.maxParitionBytes", 131072)


    // read csv file
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load("src/main/BigMarket Sales.csv")

    // df.show(5)

    println(s"Partitions: ${df.rdd.getNumPartitions}")     // prints: Partitions: 1
    val df10 = df.repartition(10)

    println(s"Partitions: ${df10.rdd.getNumPartitions}")     // prints: Partitions: 10

    // parallel
    df10.withColumn("partition_id", spark_partition_id())
      .show(10)

    // writing data
    df10.write.format("parquet")
      .mode("append")
      .option("path", "rawdata/parquetWrite")
      .save()

    spark.sparkContext.setJobDescription("Filter Tier 1 outlets")
    var df_new = spark.read.format("parquet")
      .load("rawdata/parquetWrite")

    df_new = df_new.filter(col("Outlet_Location_Type") === "Tier 1")

    df_new.show()

    // optimization
    spark.sparkContext.setJobDescription("Filter Tier 1 outlets - optmization")
    df10.write.format("parquet")
      .mode("append")
      .partitionBy("Outlet_Location_Type")
      .option("path", "/rawdata/parquetWriteOpt")
      .save()

    df_new = spark.read.format("parquet")
      .load("/rawdata/parquetWriteOpt")

    df_new = df_new.filter(col("Outlet_Location_Type") === "Tier 1")

    df_new.show()
  }
}
