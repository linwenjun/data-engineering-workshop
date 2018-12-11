package workshop.orders.handler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions

object OrderDFHandler {
  def say(dataFrame: DataFrame): Unit = {
    dataFrame.printSchema()
    dataFrame.show()
  }
}
