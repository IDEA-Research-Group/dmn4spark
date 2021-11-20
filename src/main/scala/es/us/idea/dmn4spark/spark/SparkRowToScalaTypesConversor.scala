package es.us.idea.dmn4spark.spark

import java.util

import es.us.idea.dmn4spark.spark.Utils.ColumnInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkRowToScalaTypesConversor {

  def spark2map(row: Row, dsSchema: Option[org.apache.spark.sql.types.StructType] = None):Map[String, AnyRef] = {

    def format(schema: Seq[org.apache.spark.sql.types.StructField], row: Row): Map[String, AnyRef] = {
      var result:Map[String, AnyRef] = Map()

      schema.foreach(s => //println(s.dataType)
        s.dataType match {
          case org.apache.spark.sql.types.ArrayType(elementType, _)=> val thisRow = row.getAs[mutable.WrappedArray[AnyRef]](s.name); result = result ++ Map(s.name -> formatArray(elementType, thisRow))
          case org.apache.spark.sql.types.StructType(structFields)=> val thisRow = row.getAs[Row](s.name); result = result ++ Map( s.name -> format(thisRow.schema, thisRow))
          case _ => result = result ++ Map(s.name -> row.getAs(s.name))
        }
      )
      result
    }

    def formatArray(elementType: org.apache.spark.sql.types.DataType, array: mutable.WrappedArray[AnyRef]): Seq[AnyRef] = {
      elementType match {
        case org.apache.spark.sql.types.StructType(structFields) => array.map(e => format(structFields, e.asInstanceOf[Row]))
        case org.apache.spark.sql.types.ArrayType(elementType2, _) => array.map(e => formatArray(elementType2, e.asInstanceOf[mutable.WrappedArray[AnyRef]]))
        case _ => array
      }
    }

    if(dsSchema.isDefined) format(dsSchema.get, new GenericRowWithSchema(row.toSeq.toArray, dsSchema.get) ) else format(row.schema, row)

  }

}
