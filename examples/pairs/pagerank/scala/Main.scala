import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val allLines = sc.textFile("../input")


    allLines
      .map(l => {
        val line = l.split(": ")
        val secondPart = line(1).split(", ")
        val length = secondPart.length
        val weight = 1f / length

        (secondPart, weight)

      }).flatMap(s => s._1.map(_ -> s._2))
      .reduceByKey(_ + _)
      .foreach(p => println("The New Value of " + p._1 + " is = " + (.15 +.85 * p._2)))


  }
}
