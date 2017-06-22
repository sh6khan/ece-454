import org.apache.spark.{SparkContext, SparkConf}

object Task2 {
  def countNonEmpty(s: String): Int = {
    var count = 0
    val scores = s.split(",").drop(0)

    for (i <- 1 to scores.length - 1) {
      if (scores(i).nonEmpty) {
        count = count + 1
      }
    }
    count
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => countNonEmpty(line)).reduce(_ + _)
    val out1 = output.collect()
    out1.saveAsTextFile(args(1))
  }
}
