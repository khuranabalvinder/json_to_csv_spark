
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestCsvOutPutFormatPreprocessor {


  private val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark test example")
    .getOrCreate()

  def readFile(filePath: String, fileType: String, isHeader: String, isInferSchema: String, schema: StructType): DataFrame = {

    val filePathT = getClass.getResource(filePath).getPath

    if (fileType.equalsIgnoreCase("avro")) {
      spark.read.format("com.databricks.spark.avro") load filePathT
    }
    else {
      spark.read.format("com.databricks.spark.csv").option("header", isHeader)
        .option("inferSchema", isInferSchema).schema(schema).load(filePathT)
    }

  }
}
