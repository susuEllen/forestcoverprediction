
import java.io.File

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ForestCoverTypePredictionApp {
  def main (args: Array[String]){
    println("Hello I am ForestCoverTypePredictionApp ")
    val forestPredictionApp = new ForestCoverTypePredictionApp()
    forestPredictionApp.run()
  }
}

//TODO: add log4j to remove sparkJunk

class ForestCoverTypePredictionApp {
  // TODO: put CSV into hdfs
  val currentDir = new File(".").getAbsolutePath
  Helper.printlnLoudly(currentDir)
  val inputData = s"$currentDir/src/main/resources/covtype.csv"
  //Read the raw file
  val conf = new SparkConf().setAppName("ForestCoverTypePredictionApp").setMaster("local")
  val sc = new SparkContext(conf)

  // feature extraction into labelPoint
  // split data training/ cross validation/ test (80/10/10)
  // build evaluation metric, in this case Precision and AUC
  // Tuning decision tree [impurity, depth, bias]
  // display prediction results
  def run(): Boolean = {
    val rawForestData: RDD[String] = sc.textFile(inputData)
    Helper.printlnLoudly(rawForestData.count())

    val data: RDD[LabeledPoint] = rawForestData.map {
      singleDataPoint =>
        lazy val dataArray = singleDataPoint.split(",").map(_.toDouble)
        val featureVector = dataArray.take(singleDataPoint.length - 1)
        val label = dataArray.last
        LabeledPoint(label, Vectors.parse(featureVector.toVector.toString()))
    }

    val (trainingData, crossValidationData, testData) = data.randomSplit(Array(80, 10, 10))
    val asdf = testData //???


    /*case class LabeledPoint @Since("1.0.0") (
    @Since("0.8.0") label: Double,
    @Since("1.0.0") features: Vector) {
  override def toString: String = {
    s"($label,$features)"
  }
*/




    sc.stop()
    true
  }
}

class ForestData{
  def ForestData

}

object Helper {
  def printlnLoudly(str: Any) = {
    println(s"############### $str")
  }
}

