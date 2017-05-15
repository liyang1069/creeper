package my.creep

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object BayesTest extends App {
  val name = "BayesTest"

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  
  //训练集生成,规定数据结构为LabeledPoint，1.0为类别标号，Vectors.dense(2.0, 3.0, 3.0)为特征向量
  val pos = LabeledPoint(1.0, Vectors.dense(2.0, 3.0, 3.0))//
  //特征值稀疏时，利用sparse构建
  val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(2, 1,1), Array(1.0, 1.0,1.0)))
  var l: List[LabeledPoint] = List()//利用List存放训练样本
  l = l.::(neg).::(pos)
  val training = sc.parallelize(l)
  val nb_model = NaiveBayes.train(training.toJavaRDD())
  
  //测试集生成
  val d: Array[Double] = Array(1,1,2)
  val v = Vectors.dense(d)//测试对象为单个vector，或者是ＲＤＤ化后的vector

  //朴素贝叶斯
  System.out.println(nb_model.predict(v));// 分类结果
  System.out.println(nb_model.predictProbabilities(v)); // 计算概率值
  
  
  //支持向量机
//  int numIterations = 100;//迭代次数
//  final SVMModel svm_model = SVMWithSGD.train(training.rdd(), numIterations);//构建模型
//  System.out.println(svm_model.predict(v));
//  
//  //决策树
//  Integer numClasses = 2;//类别数量
//  Map<Integer, Integer> categoricalFeaturesInfo = new HashMap();
//  String impurity = "gini";//对于分类问题，我们可以用熵entropy或Gini来表示信息的无序程度 ,对于回归问题，我们用方差(Variance)来表示无序程度，方差越大，说明数据间差异越大
//  Integer maxDepth = 5;//最大树深
//  Integer maxBins = 32;//最大划分数
//  final DecisionTreeModel tree_model = DecisionTree.trainClassifier(training, numClasses,categoricalFeaturesInfo, impurity, maxDepth, maxBins);//构建模型
//  System.out.println("决策树分类结果：");   
//  System.out.println(tree_model.predict(v));
//  
//  //随机森林
//  Integer numTrees = 3; // Use more in practice.
//  String featureSubsetStrategy = "auto"; // Let the algorithm choose.
//  Integer seed = 12345;
//  // Train a RandomForest model.
//  final RandomForestModel forest_model = RandomForest.trainRegressor(training,
//    categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);//参数与决策数基本一致，除了seed
//  System.out.println("随机森林结果：");   
//  System.out.println(forest_model.predict(v));
  sc.stop()
}