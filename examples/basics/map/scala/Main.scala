import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = sc.parallelize(Seq("Sami", "Yomna", "Noha"))
    words.map(_.toUpperCase).collect

  }
}
