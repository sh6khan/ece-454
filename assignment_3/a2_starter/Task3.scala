import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ListBuffer} 

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => {
        var ratings = line.split(",", -1);
        var keyReturn = ListBuffer.empty[(Int, Int)];

        for (i <- 1 until ratings.length) {
            if (ratings(i) != "") {
                var tup = (i+1, ratings(i).toInt);
                keyReturn += tup;
            }
        }

	keyReturn
    }).map(tuple => {
        (tuple._1, (tuple._2, 1));
    }).reduceByKey((t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2);
    }).map((tuple) => {
        var avg : Double = 0;
        avg = tuple._2._1.toDouble / tuple._2._2.toDouble;
        f"${tuple._1},${avg}%1.2f"
    });
    
    output.saveAsTextFile(args(1))
  }
}
