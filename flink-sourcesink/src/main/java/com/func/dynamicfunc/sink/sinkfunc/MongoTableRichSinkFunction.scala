package com.func.dynamicfunc.sink.sinkfunc

import com.factory.dynamicfactory.sink.MongoDynamicTableSinkFactory
import com.flink.common.dbutil.MongoDBFactory
import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{BigIntType, RowType, VarCharType}
import org.apache.flink.types.RowKind
import org.bson.Document

import scala.collection.JavaConverters._
import scala.collection.mutable

class MongoTableRichSinkFunction extends RichSinkFunction[RowData] {

  @transient var mongoClient: MongoClient = _
  var db: MongoDatabase = _
  var collection: MongoCollection[Document] = _
  var converter: DynamicTableSink.DataStructureConverter = null
  var options: ReadableConfig = null;
  var shcema: mutable.Buffer[RowType.RowField] = null;

  def this(converter: DynamicTableSink.DataStructureConverter,
           options: ReadableConfig,
           shcema: DataType) {
    this()
    this.converter = converter;
    this.options = options;
    this.shcema = shcema.getLogicalType.asInstanceOf[RowType].getFields.asScala;
  }

  override def open(parameters: Configuration): Unit = {
    mongoClient = MongoDBFactory.getMongoDBConn(
      options.get(MongoDynamicTableSinkFactory.MONGO_URL),
      options.get(MongoDynamicTableSinkFactory.MONGO_USER),
      options.get(MongoDynamicTableSinkFactory.MONGO_PASSW), // 固定admin
      "admin"
    )
    db = mongoClient.getDatabase(
      options.get(MongoDynamicTableSinkFactory.MONGO_DB))
    collection = db.getCollection(
      options.get(MongoDynamicTableSinkFactory.MONGO_COLLECTION))
  }

  override def close(): Unit = {
    mongoClient.close()
  }
  override def invoke(value: RowData, context: Context): Unit = {
    value.getRowKind match {
      case RowKind.INSERT =>
        val doc = row2document(value)
        MongoDBFactory.bulkWrite(collection, Seq(doc))
      case RowKind.UPDATE_BEFORE =>
      case RowKind.UPDATE_AFTER =>
        val doc = row2document(value)
        MongoDBFactory.bulkWrite(collection, Seq(doc))
      case _ =>
    }
  }

  private def row2document(row: RowData): Document = {
    val doc = new Document()
    for (i <- 0 to shcema.size - 1) {
      shcema(i).getType match {
        case v: VarCharType =>
          doc.append(shcema(i).getName, row.getString(i).toString)
        case b: BigIntType => doc.append(shcema(i).getName, row.getLong(i))
        case _ =>
      }
    }
    doc
  }

}
