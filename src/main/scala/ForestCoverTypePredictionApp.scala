
import java.io.File

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

object ForestCoverTypePredictionApp {
  def main (args: Array[String]){
    println("Hello I am ForestCoverTypePredictionApp ")
    val forestPredictionApp = new ForestCoverTypePredictionApp()
    forestPredictionApp.run()
  }
}

class ForestCoverTypePredictionApp {

  val currentDir = new File(".").getAbsolutePath
  Helper.printlnLoudly(currentDir)
  val inputData = s"$currentDir/src/main/resources/covtype.csv"
  //Read the raw file
  val conf = new SparkConf().setAppName("ForestCoverTypePredictionApp").setMaster("local")
  val numOfForestType = 7;

  // feature extraction into labelPoint
  // split data training/ cross validation/ test (80/10/10)
  // build evaluation metric, in this case Precision and AUC
  // Tuning decision tree [impurity, depth, bias]
  // display prediction results
  def run(): Boolean = {
    val sc = new SparkContext(conf)
    val rawForestData: RDD[String] = sc.textFile(inputData)
    Helper.printlnLoudly(rawForestData.count())

    val data: RDD[LabeledPoint] = rawForestData.map {
      singleDataPoint =>
        lazy val dataArray = singleDataPoint.split(",").map(_.toDouble)
        val featureVector = dataArray.take(singleDataPoint.length - 1)
        val label = dataArray.last -1
        LabeledPoint(label, Vectors.parse(featureVector.toVector.toString()))
    }

    val Array(trainingData, crossValidationData, testData) = data.randomSplit(Array(80, 10, 10))
    trainingData.cache()
    crossValidationData.cache()
    testData.cache()


    val numClasses = numOfForestType // because label value needs to be < numClass, all label values subtracted 1
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini" // "entropy"
    val maxDepth = 5
    val maxBins = 32

    // create a model from training data
    val decisionTreeModel = DecisionTree.trainClassifier(
      input = trainingData,
      numClasses = numClasses,
      categoricalFeaturesInfo = categoricalFeaturesInfo,
      impurity = impurity,
      maxDepth = maxDepth,
      maxBins = maxBins
    ) // what is the numTree param??

    // get some prediction value from cross validation data
    // calculate auc/ accuracy and precision

    //


    sc.stop()
    true
  }
}

//Forest Data Fields
//Id,Elevation,Aspect,Slope,Horizontal_Distance_To_Hydrology,Vertical_Distance_To_Hydrology,
// Horizontal_Distance_To_Roadways,Hillshade_9am,Hillshade_Noon,Hillshade_3pm,
// Horizontal_Distance_To_Fire_Points,Wilderness_Area1,Wilderness_Area2,Wilderness_Area3,Wilderness_Area4,
// Soil_Type1,Soil_Type2,Soil_Type3,Soil_Type4,Soil_Type5,Soil_Type6,Soil_Type7,Soil_Type8,Soil_Type9,Soil_Type10,
// Soil_Type11,Soil_Type12,Soil_Type13,Soil_Type14,Soil_Type15,Soil_Type16,Soil_Type17,Soil_Type18,Soil_Type19,
// Soil_Type20,Soil_Type21,Soil_Type22,Soil_Type23,Soil_Type24,Soil_Type25,Soil_Type26,Soil_Type27,Soil_Type28,
// Soil_Type29,Soil_Type30,Soil_Type31,Soil_Type32,Soil_Type33,Soil_Type34,Soil_Type35,Soil_Type36,Soil_Type37,
// Soil_Type38,Soil_Type39,Soil_Type40,Cover_Type

object Helper {
  def printlnLoudly(str: Any) = {
    println(s"\n############### $str\n")
  }
}

//TODO: add a step actually parse data into a case class, for better logging
// TODO: put CSV into hdfs
//TODO: add log4j to remove sparkJunk
//TODO: how to implement one hot encoding from rawdata?
