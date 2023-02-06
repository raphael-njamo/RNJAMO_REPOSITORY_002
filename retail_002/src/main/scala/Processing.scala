import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame


object Processing {
  /* warmUp 1 : Find out how many orders, how many products and how many sellers are in the data.
  How many products have been sold at least once? Which is the product contained in more orders? */
  /* warmUp 2 : How many distinct products have been sold in each day? */
  /* Exercise 1: What is the average revenue of the orders? */
  /* Exercise 2 : For each seller, the average % contribution of an order to the seller's daily quota */

  case class Exploration(products: DataFrame, sellers: DataFrame, sales: DataFrame) {
    def totalProducts: Long = products.select("product_id").distinct().count()

    def totalSellers: Long = sellers.select("seller_id").distinct().count()

    def totalOrders: Long = sales.select("order_id").distinct().count()

    def totalSales: Long = sales.select("product_id").distinct().count()

    def mostSold: String = {
      val idMostSold = sales.groupBy("product_id").count().sort(desc("count"))
        .select("product_id").first().getAs[String](0)
      val mostSold = products.where(s"product_id == $idMostSold").select("product_name")
        .first().getAs[String](0)
      return mostSold
    }

    def distinctSoldPerDay: DataFrame = {
      val distinctSoldProductId = sales.groupBy("date", "product_id").count()
        .select("date", "product_id", "count")
        .withColumnRenamed("count", "total_sale")
      val distinctSoldProduct = distinctSoldProductId.join(products, "product_id")
        .select("date", "product_name", "total_sale").sort(desc("total_sale"))
      return distinctSoldProduct
    }

    def averageRevenue: Double = {
      val productPrice = sales.join(products, "product_id")
        .select("price", "num_pieces_sold")
      val avgRev = productPrice.selectExpr("sum(num_pieces_sold*price)/sum(num_pieces_sold)")
        .first().getAs[Double](0)
      return avgRev
    }

    def avgDailyContribQuota: DataFrame = {
      val saleSeller = sales.join(sellers, "seller_id")
        .select("seller_name", "daily_target", "num_pieces_sold")
      val contribution = saleSeller.select(col("seller_name"),
        expr("num_pieces_sold/daily_target").alias("avgr"))
        .groupBy("seller_name").avg("avgr")
        .withColumnRenamed("avg(avgr)", "avg_contribution")
        .select(col("seller_name").alias("seller"),
          expr("avg_contribution * 100").alias("% avg_contribution"))
      return contribution
    }

  }


  /* Exercise 3: the 2nd most selling and the least selling persons for each product:
  those for product with product_id=0
   */
   class Ranking(products: DataFrame, sellers: DataFrame, sales: DataFrame, prod_name: String = "product_0"){
    def mostAndLeastSeller: String = {
      val saleProductSeller = sales.join(products, "product_id")
        .where(s"product_name = '$prod_name'")
        .select(col("seller_id"), col("num_pieces_sold").cast("Double"), col("product_name"))
        .join(sellers, "seller_id").drop("seller_id", "daily_target")
        .groupBy("product_name", "seller_name").sum("num_pieces_sold")
        .withColumnRenamed("sum(num_pieces_sold)", "sum_pieces_sold")
      //println(s"Sellers of the $prod_name :"); saleProductSeller.select("seller_name", "sum_pieces_sold").show()

      /*val sellerCount = saleProductSeller.select("product_name","seller_name").groupBy("product_name")
          .agg(countDistinct("seller_name").alias("unique_seller_count"))
          .sort((desc("unique_seller_count")))
        println("Unique sellers per product: "); sellerCount.show(10) */

      val secondBestSeller = saleProductSeller
        .withColumn("second_best", row_number().over(Window.orderBy(desc("sum_pieces_sold"))))
        .filter(col("second_best") === 2) //!\ when it's only one seller
        .select("seller_name")
        .first()
        .getAs[String](0)

      val leastSeller = saleProductSeller
        .sort(asc("sum_pieces_sold"))
        .select("seller_name")
        .first()
        .getAs[String](0)

      val rank = Map("second_best_seller" -> secondBestSeller, "least_seller" -> leastSeller)
      val mapper = new ObjectMapper()
      val rankJson = mapper.writeValueAsString(rank)
      return rankJson
    }

  }


}