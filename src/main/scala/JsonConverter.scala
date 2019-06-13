import java.io.{File, FileOutputStream}

import com.github.agourlay.json2Csv.Json2Csv

object JsonConverter {
  def main(args: Array[String]): Unit = {
    val output = new FileOutputStream("result-json.csv")
    Json2Csv.convert(new File("/Users/balvinder/Downloads/audits.json"), output) match {
      case Right(nb) => println(s"$nb CSV lines written to 'result-json.csv'")
      case Left(e)   => println(s"Something bad happened $e")
    }
  }
}
