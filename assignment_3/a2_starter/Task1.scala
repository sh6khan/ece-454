import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ListBuffer}

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
    val output = textFile.map(x => {
                            var ratings = x.split(",");
                            var max_rating = 0;
                            var max_users = ListBuffer.empty[Int];
                            var movieTitle = ratings(0);

                            for(i <- 1 until ratings.length) {
                                var stringInt = ratings(i);
			        if (stringInt != "") {
				var rating = stringInt.toInt;
                                if (rating > max_rating) {
                                    max_users.clear;
                                    max_rating = rating;
                                    max_users += i;
                                } else if (rating == max_rating) {
                                    max_users += i
                                }
				}	

                            }

			    println(movieTitle);

                            movieTitle + "," + max_users.mkString(",");
			    

                         })
    
    output.saveAsTextFile(args(1))
  }
}
