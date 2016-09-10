import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object ForestCoverTypePredictionApp {
  def main (args: Array[String]){
    println("Hello I am ForestCoverTypePredictionApp ")
    val forestPredictionApp = new ForestCoverTypePredictionApp()
    forestPredictionApp.run()
  }
}

class ForestCoverTypePredictionApp {
  // TODO: put CSV into hdfs
  val inputData = "/Users/ellenwong/IdeaProjects/ForestCoverTypePrediction/src/main/resources/covtype.csv"
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
    println(rawForestData.count())
    sc.stop()
    true
  }
}