// import package
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StopWordsRemover

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count")
   
    val sc = new SparkContext(conf)
    print("input file location is: " + args(0))

    // Open input file
    val tweets = sc.textFile(args(0)).map(_.toLowerCase)
    val tweet_words = tweets.flatMap(x=>x.split(" "))
    
    // Removing StopWords
    val stopWords = new StopWordsRemover().getStopWords
    val removeStopWords = tweet_words.filter(each => !stopWords.contains(each))

    // Setting regular expression & Removing special character
    val reg = "[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"<,>.?`/\\\\-]".r
    val removeSpecialChar = removeStopWords.filter(each => !reg.findFirstIn(each).isDefined)
    
    // Removing words whose length is more than or equal to four char    
    val lastProcessing = removeSpecialChar.filter(each => (each.length >= 4))
    val rmMap = lastProcessing.map(x => (x, 1))

    // Reduce by Key & Sort
    val lastWord = rmMap.reduceByKey((x,y) => x + y)
    val outPut = lastWord.sortBy(_._2, false)
    
    // Save as text file
    outPut.saveAsTextFile(args(1))
  }
}


