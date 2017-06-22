import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => {
        var ratings = line.split(",", -1).drop(1);

        var keyReturn = new ListBuffer.empty[Tuple];

        for (i <- 0 until ratings.length) {
            if (ratings(i) != "") {
                keyReturns += (i, ratings(i).toInt);
            }
        }
    }).map(tuple => {
        (tuple._1, (tuple._2, 1));
    }).reduceByKey((t1, t2) => {
        (t1._1 + t2._1, t1._1 + t2._1);
    }).map((tuple => {
        var avg : Double = 0;
        avg = tuple._2._1.toDouble / tuple._2._2.toDouble;
        f"$(tuple._1),$(avg)%1.2f"
    });
    
    output.saveAsTextFile(args(1))
  }
}
