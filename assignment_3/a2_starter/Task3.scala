import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def mapping(line: String): Unit = {
    val arr = line.split(",")

    for (i <- 1 to arr.length - 1) {

    }


  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => mapping(line))

    output.saveAsTextFile(args(1))
  }
}
