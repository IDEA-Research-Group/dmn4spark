package es.us.idea.dmn4spark.spark

import java.util

import es.us.idea.dmn4spark.spark.Utils.ColumnInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkDataConversor {

  def spark2javamap(row: Row, dsSchema: Option[org.apache.spark.sql.types.StructType] = None): java.util.HashMap[String, AnyRef] = {

    def format(schema: Seq[org.apache.spark.sql.types.StructField], row: Row): java.util.HashMap[String, AnyRef] = {
      val result = new java.util.HashMap[String, AnyRef]()

      schema.foreach(s =>
        s.dataType match {
          case org.apache.spark.sql.types.ArrayType(elementType, _) => val thisRow = row.getAs[mutable.WrappedArray[AnyRef]](s.name); result.put(s.name, formatArray(elementType, thisRow))
          case org.apache.spark.sql.types.StructType(structFields) => val thisRow = row.getAs[Row](s.name); result.put(s.name, format(thisRow.schema, thisRow))
          case _ => result.put(s.name, row.getAs(s.name))
        }
      )
      result
    }

    def formatArray(elementType: org.apache.spark.sql.types.DataType, array: mutable.WrappedArray[AnyRef]): util.List[AnyRef] = {

      elementType match {
        case org.apache.spark.sql.types.StructType(structFields) => array.map(e => format(structFields, e.asInstanceOf[Row]).asInstanceOf[AnyRef] ).asJava
        case org.apache.spark.sql.types.ArrayType(elementType2, _) => array.map(e => formatArray(elementType2, e.asInstanceOf[mutable.WrappedArray[AnyRef]]).asInstanceOf[AnyRef]   ).asJava
        case _ => array.asJava
      }
    }

    if(dsSchema.isDefined) format(dsSchema.get, new GenericRowWithSchema(row.toSeq.toArray, dsSchema.get) ) else format(row.schema, row)

  }

  def javamap2Spark(map: java.util.Map[String, AnyRef], columnsInfo: List[ColumnInfo])  = {
    columnsInfo.map(ci => {
      val value = Utils.unwrap(map.getOrDefault(ci.name, None))
      value match {
        case None => None
        case x => Some(x.toString)
      }
    })
  }





}
