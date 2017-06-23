import org.apache.spark.{SparkContext, SparkConf}

object Task4 {
  def calc(mv1: String, mv2: String): String = {
    if (mv1 == mv2 || mv1.compareTo(mv2) >= 0) {
      return ""
    } else {
      var arr1 = mv1.split(",")
      var arr2 = mv2.split(",")

      var ret = mv1(0) + "," + mv2(0)

      arr1i = arr1.drop(1).map(a => if (a.nonEmpty) a.toInt else 0)
      arr2i = arr2.drop(1).map(b => if (b.nonEmpty) b.toInt else 0)


      val mag1 = Math.sqrt(arr1i.map(x => Math.pow(x, 2)).sum)
      val mag2 = Math.sqrt(arr2i.map(y => Math.pow(y, 2)).sum)

      var dot = 0
      for (i <- 0 to arr1i.length -1){
        dot = dot + arr1i(i) * arr2i(i)
      }
      val cosine = dot / (mag1 * mag2)
      ret = ret + "," + f"$cosine%1.2f"
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.cartesian(textFile).map(pair => calc(pair._1, pair._2)).filter(_.nonEmpty)

    output.saveAsTextFile(args(1))
  }
}
