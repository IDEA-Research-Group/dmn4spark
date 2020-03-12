package es.us.idea.dmn4spark.spark

import org.apache.spark.sql.types.{DataType, DataTypes}

object Utils {

  case class ColumnInfo(name: String, dataTypeWithAssignedValue: DataTypeWithAssignedValue)
  case class DataTypeWithAssignedValue(dataType: DataType, establishedValue: Option[Boolean] = None) extends Serializable

  def createStructType(fieldNames: Seq[String]) = {
    DataTypes.createStructType(fieldNames.map(DataTypes.createStructField(_, DataTypes.StringType, true)).toArray)
  }

  /**
    *
    * @param str string containing the part of the rule consequence which includes the second argument of the
    *            insertResult function
    * @return the data type of the second argument of the inserResult function and, if it is Boolean, its associated
    *         value
    */
  def inferSecondArgumentType(str: String): DataTypeWithAssignedValue = {
    str.split(')').headOption match {
      case Some(x) =>
        if(x.contains("\"")) DataTypeWithAssignedValue(DataTypes.StringType)
        else {
          val candidate = x.split("\\s+").mkString//.asInstanceOf[Any]
          if(candidate == "true") DataTypeWithAssignedValue(DataTypes.BooleanType, Some(true))
          else if(candidate == "false") DataTypeWithAssignedValue(DataTypes.BooleanType, Some(false))
          else if(candidate.matches("^(\\d*)$")) DataTypeWithAssignedValue(DataTypes.IntegerType)
          else if(candidate.matches("^(\\d*)L$")) DataTypeWithAssignedValue(DataTypes.LongType)
          else if(candidate.matches("^(\\d+\\.\\d*|\\.?\\d+)$")) DataTypeWithAssignedValue(DataTypes.DoubleType)
          else if(candidate.matches("^(\\d+\\.\\d*|\\.?\\d+)f$")) DataTypeWithAssignedValue(DataTypes.FloatType)
          else DataTypeWithAssignedValue(DataTypes.StringType)
        }

      case _ => DataTypeWithAssignedValue(DataTypes.StringType)
    }
  }

  def unwrap(a: Any): Any = {
    a match {
      case Some(x) => unwrap(x)
      case null => None
      case _ => a
    }
  }

}
