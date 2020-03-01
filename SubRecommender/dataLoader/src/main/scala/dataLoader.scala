import java.net.InetAddress

import MyESUtil.insertBulk
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * Movie 数据集
 *
 * 260                                         电影ID，mid
 * Star Wars: Episode IV - A New Hope (1977)   电影名称，name
 * Princess Leia is captured and held hostage  详情描述，descri
 * 121 minutes                                 时长，timelong
 * September 21, 2004                          发行时间，issue
 * 1977                                        拍摄时间，shoot
 * English                                     语言，language
 * Action|Adventure|Sci-Fi                     类型，genres
 * Mark Hamill|Harrison Ford|Carrie Fisher     演员表，actors
 * George Lucas                                导演，directors
 *
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
 * Rating数据集
 *
 * 1,31,2.5,1260759144
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int )

/**
 * Tag数据集
 *
 * 15,1955,dentist,1193435061
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

// 把mongo和es的配置封装成样例类

/**
 *
 * @param uri MongoDB连接
 * @param db  MongoDB数据库
 */
case class MongoConfig(uri:String, db:String)

/**
 *
 * @param httpHosts       http主机列表，逗号分隔
 * @param transportHosts  transport主机列表
 * @param index            需要操作的索引
 * @param clustername      集群名称，默认elasticsearch
 */
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)



object dataLoader {
    // 定义相关常量
    val MOVIE_DATA_PATH = "H:\\机器学习和推荐系统\\3.代码\\recommender\\SubRecommender\\dataLoader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH = "H:\\机器学习和推荐系统\\3.代码\\recommender\\SubRecommender\\dataLoader\\src\\main\\resources\\ratings.csv"
    val TAG_DATA_PATH = "H:\\机器学习和推荐系统\\3.代码\\recommender\\SubRecommender\\dataLoader\\src\\main\\resources\\tags.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val ES_MOVIE_INDEX = "Movie"



    def main(args: Array[String]): Unit = {
        // 定义用到的配置参数
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://192.168.6.102:27017/recommender",
            "mongo.db" -> "recommender",
            "es.httpHosts" -> "192.168.6.106:9200",
            "es.transportHosts" -> "192.168.6.106:9300",
            "es.index" -> "recommender",
            "es.cluster.name" -> "elasticsearch"
        )
        val sparkConf = new
                SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        //将MovieRDD装换为DataFrame
        val movieDF = movieRDD.map(item =>{
            val attr = item.split("\\^")
            Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,
                attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
        }).toDF()

        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
        //将ratingRDD转换为DataFrame
        val ratingDF = ratingRDD.map(item => {
            val attr = item.split(",")
            Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
        }).toDF()

        val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
        //将tagRDD装换为DataFrame
        val tagDF = tagRDD.map(item => {
            val attr = item.split(",")
            Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
        }).toDF()

        // 声明一个隐式的配置对象

        implicit val mongoConfig =
            MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)
        // 将数据保存到MongoDB中
        storeDataInMongoDB(movieDF, ratingDF, tagDF)
        import org.apache.spark.sql.functions._
        val newTag = tagDF.groupBy($"mid")
            .agg(concat_ws("|",collect_set($"tag"))
                .as("tags"))
            .select("mid","tags")
        // 需要将处理后的Tag数据，和Moive数据融合，产生新的Movie数据
        val movieWithTagsDF = movieDF.join(newTag,Seq("mid","mid"),"left")

        // 声明了一个ES配置的隐式参数
        implicit  val esConfig = ESConfig(config.get("es.httpHosts").get,
            config.get("es.transportHosts").get,
            config.get("es.index").get,
            config.get("es.cluster.name").get)
        // 需要将新的Movie数据保存到ES中
     //   storeDataInES(movieWithTagsDF)
     val rdd = movieWithTagsDF.rdd.collect().toList
        insertBulk("recommender", rdd)
        // 关闭Spark
        spark.stop()


    }
    def storeDataInMongoDB(movieDF: DataFrame, ratingDF:DataFrame, tagDF:DataFrame)
                          (implicit mongoConfig: MongoConfig): Unit = {

        //新建一个到MongoDB的连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
        //如果MongoDB中有对应的数据库，那么应该删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

        //将当前数据写入到MongoDB
        movieDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_MOVIE_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        ratingDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        tagDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_TAG_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        //对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        //关闭MongoDB的连接
        mongoClient.close()
    }

}


