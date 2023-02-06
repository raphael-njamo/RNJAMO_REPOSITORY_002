
import Processing.{Ranking, Exploration}
import Ingestion.getData

object main extends App {
  println(" Start ingesting data...")
  val dataset = new getData("C:/Users/njamo/OneDrive/Documents/bigdata/output/DatasetToCompleteTheSixSparkExercises/")
  val products = dataset.products(100)
  val sellers = dataset.sellers
  val sales = dataset.sales(100)
  println(" Data successful loaded \t")

  println(" Start processing...")
  val exporation = new Exploration(products,sellers,sales)
  val ranking = new Ranking(products,sellers,sales, "product_20774718")

  println(exporation.mostSold)
  println(exporation.distinctSoldPerDay.show())

  println(ranking.mostAndLeastSeller)
  println(" Processing ended!")

}
