import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, DocumentResult, Index}

import scala.collection.mutable

/**
 * Author lpc
 * Date 2019/5/15 6:50 PM
 */
object MyESUtil {
    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory: JestClientFactory = _

    /**
     * 构建客户端工厂对象
     */
    def buildFactory(): Unit = {
        val config: HttpClientConfig = new HttpClientConfig.Builder(s"$ES_HOST:$ES_HTTP_PORT")
            .multiThreaded(true)
            .maxTotalConnection(20)
            .connTimeout(10000)
            .readTimeout(10000)
            .build()
        factory = new JestClientFactory()
        factory.setHttpClientConfig(config)
    }

    /**
     * 获取客户端对象
     *
     * @return
     */
    def getClient(): JestClient = {
        if (factory == null) buildFactory()
        factory.getObject
    }

    /**
     * 关闭客户端
     *
     * @param client
     */
    def closeClient(client: JestClient) = {
        if (client != null) {
            try {
                client.shutdownClient()
            } catch {
                case e => e.printStackTrace()
            }
        }
    }

    /**
     * 批量插入数据
     * 插入的时候保证至少传一个 source 进来
     *
     * @param index
     * @param doc
     * @param source
     * @param otherSource
     */
    def insertBulk(index: String, sources: Iterable[Any]) = {
        val bulkBuilder = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
        sources.foreach(any => {
            bulkBuilder.addAction(new Index.Builder(any).build())
        })
        val client: JestClient = getClient()
        client.execute(bulkBuilder.build())
        closeClient(client)
    }
    def main(args: Array[String]): Unit = {
        /*val client: JestClient = getClient()
        //        singleOperation(client)
        multiOperation(client)
        closeClient(client)*/


        val source1 = Startup("mid777111", "uid222", "", "", "", "", "", "", "", "", "", 123124141)
        val source2 = Startup("mid888222", "uid333", "", "", "", "", "", "", "", "", "", 123124141)
        insertBulk("gmall_dau", source1::source2::Nil)
    }

    /**
     * 批量操作测试
     *
     * @param client
     * @return
     */
    def multiOperation(client: JestClient) = {
        val source1 = Startup("mid777", "uid222", "", "", "", "", "", "", "", "", "", 123124141)
        val source2 = Startup("mid888", "uid333", "", "", "", "", "", "", "", "", "", 123124141)
        val bulkBuilder = new Bulk.Builder().defaultIndex("recommender").defaultType("_doc")
        bulkBuilder.addAction(new Index.Builder(source1).build())
        bulkBuilder.addAction(new Index.Builder(source2).build())
        client.execute(bulkBuilder.build())
    }

    /**
     * 单次操作测试
     *
     * @param client
     * @return
     */
    private def singleOperation(client: JestClient): DocumentResult = {
        // 测试
        // 1. 写入操作(单个)   index? type?  内容?
        // 1.1 插入json 格式的字符串
        val source1 =
        """
          |{
          |  "mid": "mid_1234",
          |  "uid": "123"
          |}
        """.stripMargin
        // 1.2 插入样例类
        val source2 = Startup("mid777", "uid222", "", "", "", "", "", "", "", "", "", 123124141)
        val index: Index = new Index.Builder(source2)
            .index("gmall_dau")
            .`type`("_doc")
            .build()
        client.execute(index)
    }
}

case class Startup(mid: String,
                   uid: String,
                   appid: String,
                   area: String,
                   os: String,
                   ch: String,
                   logType: String,
                   vs: String,
                   var logDate: String,
                   var logHour: String,
                   var logHourMinute: String,
                   var ts: Long
                  ) {

}

