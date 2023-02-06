
import Logging.Connexion

object Ingestion{

  class getData(rootPath: String) {
    val connex = new Connexion
    val spark = connex.getSparkSession

    def products(limit: Int=75000000): org.apache.spark.sql.DataFrame = return spark.read.format("parquet")
      .load(rootPath + "products_parquet/*.parquet")
      .limit(limit)

    def sales(limit: Int=20000000): org.apache.spark.sql.DataFrame  = return spark.read.format("parquet")
      .load(rootPath + "sales_parquet/*.parquet")
      .limit(limit)

    def sellers: org.apache.spark.sql.DataFrame  = return spark.read.format("parquet")
      .load(rootPath + "sellers_parquet/*.parquet")

  }
  /*
  val getDatas = new getData("C:/Users/njamo/OneDrive/Documents/bigdata/output/DatasetToCompleteTheSixSparkExercises/")
  val prod = getDatas.products(100)
  val sel = getDatas.sellers
  val sal = getDatas.sales(100)
  prod.show(2)
  sel.show(2)
  sal.show(2)

   */

}
