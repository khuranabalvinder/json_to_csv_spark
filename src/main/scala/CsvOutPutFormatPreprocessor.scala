import java.util.regex.Pattern

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable.ListBuffer


class CsvOutPutFormatPreprocessor {

  def flattenNestedStructure(jsonDataFrame: DataFrame): Array[Column] = {
    val jsonSchema = jsonDataFrame.schema
    println(jsonDataFrame.schema)
    val list = ListBuffer[String]()
    val listOfSimpleType = ListBuffer[String]()

    for (structField <- jsonSchema.fields) {
      if (structField.dataType.toString.startsWith("StructType")) {
        val prefix = structField.name
        val matchFound = Pattern.compile("(\\w+\\(([A-Z_a-z_0-9]+),\\w+,\\w+\\))+").matcher(structField.dataType.toString)
        while ( {
          matchFound.find
        }) list += prefix + "." + matchFound.group(2)
      }
      else listOfSimpleType += (structField.name)
    }
    var i = 0
    val column = new Array[Column](list.size + listOfSimpleType.size)
    for (columnName <- listOfSimpleType) {
      column(i) = col(columnName)
      i += 1
    }
    for (column_name <- list) {
      column(i) = col(column_name).alias(column_name)
      i += 1
    }
    column
  }
}