package workshop.orders.handler

import org.apache.spark.sql.DataFrame
import workshop.DefaultFeatureSpecWithSpark

class OrderDFHandlerTest extends DefaultFeatureSpecWithSpark {
  feature("Word Count application") {
    scenario("Acceptance test for basic use") {
      Given("A simple input file, a Spark context, and a known output file")
      import spark.implicits._

      val df = Seq(
        ("1", "First Value", "abc"),
        ("2", "Second Value", "ccc")
      ).toDF("ID", "VALUE", "KEY")

      OrderDFHandler.say(df)

      getValueFromDF(df, 1, "VALUE") should equal("Second Value")

    }
  }

  def getValueFromDF (df:DataFrame, row:Int, field:String): String = {
    val list = df.select(field).limit(100).collectAsList()
    list.get(row).get(0).toString()
  }
}
