import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile("../input")

    input.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .foreach(pair => println("The Word " + pair._1 + " repeated " + pair._2 + " times"))

  }
}
