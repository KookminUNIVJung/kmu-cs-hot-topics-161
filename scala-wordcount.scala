import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    print("input file location is: " + args(0))
    val tweets = sc.textFile(args(0))
    val tweet_words = tweets.flatMap(x=>x.split("\\s+"))
    val kv = tweet_words.map(x => (x, 1))
    val word_count = kv.reduceByKey((x,y) => x+y)
    val top_ten = word_count.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))
    top_ten.foreach(println)
  }
}
