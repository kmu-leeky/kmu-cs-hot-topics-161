import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StopWordsRemover

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val tweets = sc.textFile(args(0))

    val stop_words = new StopWordsRemover().getStopWords
    val tweet_words = tweets.flatMap(x=>x.toLowerCase().replaceAll("[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"<,>.?'/\\\\-]","").split("\\s+")).filter(x=>x.length>=4).filter(!stop_words.contains(_))

    val kv = tweet_words.map(x => (x, 1))
    val word_count = kv.reduceByKey((x,y) => x+y)

    val descend_order = word_count.map{case (x,y)=>(y,x)}.sortByKey(false,1).map{case (x,y)=>(y,x)}
    val top10 = sc.parallelize(descend_order.take(10))
    top10.coalesce(1,true).saveAsTextFile(args(1))
  }
}

