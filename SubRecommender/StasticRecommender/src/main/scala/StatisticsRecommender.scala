import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

case class Recommendation(mid:Int, score:Double)
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])


object StatisticsRecommender {
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_COLLECTION = "Movie"

    //统计的表的名称
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://192.168.6.102:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
        //创建SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //加入隐式转换
        import spark.implicits._


        val ratingDF = spark
            .read
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Rating]
            .toDF()
        val movieDF = spark
            .read
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Movie]
            .toDF()
        // 历史热门统计
        ratingDF.createOrReplaceTempView("ratings")
        val rateMoveDF: DataFrame = spark.sql("select mid,count(mid) as count from ratings group by mid")
        rateMoveDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",RATE_MORE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        // 最近热门统计
        val dataFormat = new SimpleDateFormat("yyyyMM")

        spark.udf.register("changDateFormat",(date:Int)=>{
            dataFormat.format(date.toLong)
        })


        val df: DataFrame = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
        df.createOrReplaceTempView("ratingOfMonth")
        spark.sql("select mid, count(mid) as count ,yearmonth from ratingOfMonth group by yearmonth,mid")
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",RATE_MORE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")

        averageMoviesDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",AVERAGE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        val movieWithScore= movieDF.join(averageMoviesDF, "mid")
        val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
            ,"Romance","Science","Tv","Thriller","War","Western")

//case class Movie(mid: Int,// name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)
         val genresRDD = spark.sparkContext.makeRDD(genres)
        val genrenTopMovies = genresRDD.cartesian(movieWithScore.rdd)
            .filter{
                        //moviewithsocre包含所笛卡尔的这个电影
                case (genres,row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
            }
            .map{
                // 将整个数据集的数据量减小，生成RDD[String,Iter[mid,avg]]
                case (genres,row) => {
                    (genres,(row.getAs[Int]("mid"), row.getAs[Double]("avg")))
                }
            }.groupByKey()
            .map{
                case (genres, items) => GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2)
                    .take(10)
                    .map(item => Recommendation(item._1,item._2)))
            }.toDF()
        genrenTopMovies
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",GENRES_TOP_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()





    }

}
