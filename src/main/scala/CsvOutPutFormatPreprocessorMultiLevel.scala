import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable.ListBuffer


class CsvOutPutFormatPreprocessorMultiLevel {

  def flattenNestedStructure(jsonDataFrame: DataFrame): Array[Column] = {
    val jsonSchema = jsonDataFrame.schema
    println(jsonDataFrame.schema)

    val columns = ListBuffer[Column]()
    flattenSchema(jsonSchema.fields, columns, "", 0)
    columns.toArray
  }


  private def flattenSchema(jsonFields: Array[StructField], columns: ListBuffer[Column],
                            superPrefix: String, arraySize: Int): Unit = {
    for (structField <- jsonFields) {
      val prefix = if (superPrefix.length() > 0)
        superPrefix + "." + structField.name
      else
        structField.name
      if (structField.dataType.toString.startsWith("StructType")) {
        flattenSchema(structField.dataType.asInstanceOf[StructType].fields, columns, prefix, 0)
      }
      else if (structField.dataType.toString.startsWith("ArrayType")) {
        val size = structField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields.size
        //flattenSchema(structField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields, columns, prefix, size)
      }
      else {
        columns += col(prefix).alias(prefix)
      }
    }
  }
}