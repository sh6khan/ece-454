import org.apache.spark.{SparkContext, SparkConf}

object MyFuncs {
  def solve(line: String): String = {
    val s = line.split(",")
    var max = -1
    for (i <- 1 to s.length) {
      if (s(i).toInt > max) {
        max = s(i).toInt()
      }
    }
    var ret = s[0]
    for (i <- 1 to s.length) {
      if (s(i).toInt == max){
        ret = ret + "," + i.toString
      }
    }
    ret
  }
}

object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => solve(line))
    
    output.saveAsTextFile(args(1))
  }
}
