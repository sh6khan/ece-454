import org.apache.spark.{SparkContext, SparkConf}

object Task4 {
  def calc(arr1: Array[String], arr2: Array[String]): String = {
        val arr1i = arr1.drop(1).map(a => if (a.nonEmpty) a.toInt else 0)
        val arr2i = arr2.drop(1).map(b => if (b.nonEmpty) b.toInt else 0)

        var mag1 = 0
        var mag2 = 0
        var dot = 0
        for (i <- 0 to arr1i.length - 1){
          dot = dot + arr1i(i) * arr2i(i)
          mag1 += arr1i(i) * arr1i(i)
          mag2 += arr2i(i) * arr2i(i)
        }
        val cosine = dot / (Math.sqrt(mag1) * Math.sqrt(mag2))
        arr1(0) + "," + arr2(0) + "," + f"$cosine%1.2f"
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val lines = textFile.map(line => line.split(",", -1))
    val output = lines.cartesian(lines)
      .filter(pair => pair._1(0).compareTo(pair._2(0)) < 0)
      .map(pair => calc(pair._1, pair._2))

    output.saveAsTextFile(args(1))
  }
}


