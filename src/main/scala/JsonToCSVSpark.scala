import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
object JsonToCSVSpark {
  def main(args: Array[String]) {

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
    val json_path = "/Balvinder/METRO/axis_poc/src/main/scala/samplejson2witharray.json"
    //val json_path = "/Users/balvinder/Downloads/audits.json"
    val df = sparkSession.read.option("multiLine", true).option("mode", "PERMISSIVE").json(json_path)
    val csvOutPutFormatPreprocessor = new CsvOutPutFormatPreprocessorMultiLevel
    sparkSession.sql("set spark.sql.caseSensitive=true")
    val flattened_column: Array[Column] = csvOutPutFormatPreprocessor.flattenNestedStructure(df)
    //val df2 = df.select("sources[0].sourceId")
    val df1 = df.select(flattened_column :_*)
      //.write.mode(SaveMode.Overwrite).option("header", "true").format("csv").save("/Users/balvinder/Downloads/audits_sparkcsv.csv")
    df1.write.format("com.databricks.spark.csv").option("header", "true").save("/Balvinder/METRO/axis_poc/src/main/scala/samplecsv2.csv")
  }
}


