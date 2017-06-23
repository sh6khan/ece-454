import org.apache.spark.{SparkContext, SparkConf}

object Task4 {
  def calc(arr1: Array[String], arr2: Array[String]): String = {
      if (arr1(0).compareTo(arr2(0)) >= 0) {
        return ""
      } else {
        val arr1i = arr1.drop(1).map(a => if (a.nonEmpty) a.toInt else 0)
        val arr2i = arr2.drop(1).map(b => if (b.nonEmpty) b.toInt else 0)

        val mag1 = Math.sqrt(arr1i.map(x => Math.pow(x, 2)).sum)
        val mag2 = Math.sqrt(arr2i.map(y => Math.pow(y, 2)).sum)

        var dot = 0
        for (i <- 0 to arr1i.length -1){
          dot = dot + arr1i(i) * arr2i(i)
        }
        val cosine = dot / (mag1 * mag2)
        arr1(0) + "," + arr2(0) + "," + f"$cosine%1.2f"
      }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val lines = textFile.map(line => line.split(",", -1))
    val output = lines.cartesian(lines)
      .map(pair => calc(pair._1, pair._2))
      .filter(_.nonEmpty)

    output.saveAsTextFile(args(1))
  }
}
