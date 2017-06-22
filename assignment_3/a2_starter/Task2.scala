import org.apache.spark.{SparkContext, SparkConf}

object Task2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => line.split(",").drop(1).filter(_.nonEmpty))
      .count
    sc.parallelize(Seq(output)).coalesce(1).saveAsTextFile(args(1))}
}
