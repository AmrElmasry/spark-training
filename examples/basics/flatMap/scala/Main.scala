import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(Seq("Hello World", "Spark Course"))
    input.flatMap(_.split(" ")).collect

  }
}
