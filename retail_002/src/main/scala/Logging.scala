import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Logging {
  class Connexion {
    def getSparkSession: SparkSession = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder()
        .master("local")
        .appName("rnjamo_001")
        .config("spark.executor.instances", 8)
        .getOrCreate()
      return spark
    }
 }

}


