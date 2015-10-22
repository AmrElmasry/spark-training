import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val group1 = sc.parallelize(Seq(10, 20, 30))
    val group2 = sc.parallelize(Seq(10, 40, 50))

    group1.union(group2).collect

  }
}
