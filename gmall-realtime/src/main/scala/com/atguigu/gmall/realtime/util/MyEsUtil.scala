package com.atguigu.gmall.realtime.util

import java.util

import com.atguigu.gmall.common.constant.GmallConstant
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}


object MyEsUtil {

  private val ES_HOST: String = PropertiesUtil.getProperties(GmallConstant.GMALL_REALTIME_CONFIG).getProperty("elasticsearch.host")

  private val ES_PORT: String = PropertiesUtil.getProperties(GmallConstant.GMALL_REALTIME_CONFIG).getProperty("elasticsearch.port")

  private var factory: JestClientFactory = _

  private def initFactory(): JestClientFactory = {
    factory = new JestClientFactory()
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder(ES_HOST + ":" + ES_PORT).multiThreaded(true)
        .maxTotalConnection(20).connTimeout(10000)
        .readTimeout(1000).build()
    )
    factory
  }


  private def getClient(): JestClient = {
    if (factory == null) initFactory()
    factory.getObject
  }

  private def closeClient(client: JestClient) = {
    if (client != null) client.close()
  }

  def insert(source: Any,indexName: String, typeName: String) = {
    if (source != null) {
      val jest: JestClient = getClient()
      try {
        val index: Index = new Index.Builder(source).index(indexName).`type`(typeName).build()
        jest.execute(index)
      } catch {
        case ex: Exception => println(ex.getMessage)
      } finally {
        closeClient(jest)
      }
    }
  }


  def insertBulks(sourceList: List[(String, Any)], indexName: String, typeName: String) = {
    if (sourceList != null && sourceList.nonEmpty) {
      val jest: JestClient = getClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName)
      for ((id, source) <- sourceList) {
        val indexBuilder: Index.Builder = new Index.Builder(source)
        if (id != null) indexBuilder.id(id)
        bulkBuilder.addAction(indexBuilder.build())
      }
      val bulk: Bulk = bulkBuilder.build()
      var items: util.List[BulkResult#BulkResultItem] = new util.LinkedList[BulkResult#BulkResultItem]()
      try {
        items = jest.execute(bulk).getItems
      } catch {
        case exception: Exception => println(exception.toString)
      } finally {
        closeClient(jest)
        println(s"保存${items.size()}条数据!")
      }
    }
  }





  def main(args: Array[String]): Unit = {
   val studs: List[Stud] = List(Stud("zhang1","1"),Stud("zhang2","2"),Stud("zhang3","3"),Stud("zhang4","4"),Stud("zhang5","5"))
    val ids: Range = (1 to studs.length)
    var id: Int = 0
    val mapList: List[(String, Stud)] = studs.map(stud => {
      val tuple: (String, Stud) = (ids(id).toString, stud)
      id = id + 1
      tuple
    })
    insertBulks(mapList,"gmall_stud","stud")
  }

  case class Stud(name: String, age: String)
}
