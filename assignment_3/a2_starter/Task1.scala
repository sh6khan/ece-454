import org.apache.spark.{SparkContext, SparkConf}

object MyFuncs {
  def getMax(s: Array[String]): Unit = {
    val out = 1
  }
}

object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => line.split(""))
    
    output.saveAsTextFile(args(1))
  }
}
