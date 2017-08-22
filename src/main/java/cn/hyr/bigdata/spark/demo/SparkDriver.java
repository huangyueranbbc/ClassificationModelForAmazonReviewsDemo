package cn.hyr.bigdata.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * @category 亚马逊评论的分类模型 classification model for Amazon reviews
 * @author huangyueran
 *
 */
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;

//		inputPath=args[0]; // 输入数据集文件路径
		inputPath="data.txt";
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab8").setMaster("local[4]");
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Create a Spark SQL Context object
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
		
        	//EX 1: READ AND FILTER THE DATASET AND STORE IT INTO A DATAFRAME
		
		// To avoid parsing the comma escaped within quotes, you can use the following regex:
		// line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		// instead of the simpler
		// line.split(",");
		// this will ignore the commas followed by an odd number of quotes.
		
		JavaRDD<String> input = sc.textFile(inputPath); 
		
		/**
		 * 筛选评分>0的有效评论
		 */
		JavaRDD<String> inputFiltered = input.filter(new Function<String, Boolean>() {
			StringBuffer s=new StringBuffer();
			public Boolean call(String line) throws Exception {
//				System.out.println(line);
				
				String[] fields = line.split("\t");
				
				if(line.startsWith("product/productId") == true) {
					try {
						if(Double.parseDouble(fields[4].split(":")[1]) > 0) {
							return true; 
						}
					} catch (Exception e) {
						return false;
					}
				
				}; 
				return false;
			}
		}).cache();
		
		/**
		 * 获取指定用户的评论 用户名:用户评论的所有信息
		 */
		JavaPairRDD<String, String> linesForUser = inputFiltered.mapToPair(new PairFunction<String, String, String>() {

			
			public Tuple2<String, String> call(String line) throws Exception {
				// TODO Auto-generated method stub
//				String[] fields = arg0.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				String[] fields = line.split("\t");
				return new Tuple2<String, String>(fields[2], line);  // 用户名:用户评论的所有信息
			}
			
		});

		/**
		 * 筛选有用的评论  返回的元组的参数二:元组第一个是评论主体数,第二个是有效评论数。也就是计算用户的权重?
		 */
		JavaPairRDD<String, Tuple2<Integer, Integer>> usersMap = inputFiltered.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {

			
			public Tuple2<String, Tuple2<Integer, Integer>> call(String line) throws Exception {
				// TODO Auto-generated method stub
				Tuple2 t;
//				String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				String[] fields = line.split("\t");
//				System.out.println((Double.parseDouble(fields[3].split(":")[1].split("/")[0]) / Double.parseDouble(fields[3].split(":")[1].split("/")[1]) ) );
				// 取出有用的评价(有用度>0.9)
				if( (Double.parseDouble(fields[3].split(":")[1].split("/")[0]) / Double.parseDouble(fields[3].split(":")[1].split("/")[1]) ) >= 0.9) { // 假设:判断有帮助度>0.9 review/helpfulness: 6/6
					t = new Tuple2<Integer, Integer>(1, 1);
				} else {
					t = new Tuple2<Integer, Integer>(1, 0);
				}
				return new Tuple2<String, Tuple2<Integer, Integer>>(fields[2], t); // ( 用户名:(评论主体数:有效评论数) )  计算单个用户总评论数
				//  Tuple2<Integer, Integer> 用户评论主题数:用户有效评论数
			}
			
		});
		
		/**
		 * reduceByKey根据KEY合并 统计用户评论总数和有效评论数
		 */
		JavaPairRDD<String, Tuple2<Integer, Integer>> usersCount = usersMap.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

			
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2); //计算用户评论的主题数和用户有效评论数
			}	
		});
		
		/**
		 * 将(用户名:用户评论信息) 和 (用户名:(评论主题数:有效评论数)) 进行合并 (用户名,用户评论信息,(评论主题数:有效评论数))
		 */
		JavaPairRDD<String, Tuple2<String, Tuple2<Integer, Integer>>> inputWithNewFields = linesForUser.join(usersCount); //(用户名,用户评论信息,(评论主题数:有效评论数))
		
		//				inputWithNewFields.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Tuple2<Integer,Integer>>>>() {
		//					
		//					public void call(Tuple2<String, Tuple2<String, Tuple2<Integer, Integer>>> t) throws Exception {
		//							System.out.println(t._1+":("+t._2._1+":"+t._2._2+")");
		//					}
		//				});
		
		// 特征:
		// 长文本
		// 用户的评审次数
		// 对用户有用的评论数
		// 分数
		/**
		 * 训练
		 */
		JavaRDD<LabeledPoint> trainingRDD = inputWithNewFields.map(new Function<Tuple2<String, Tuple2<String, Tuple2<Integer, Integer>>>, LabeledPoint>() {

			// joinData ==>>  (用户名,用户评论信息,(评论主题数:有效评论数))
			public LabeledPoint call(Tuple2<String, Tuple2<String, Tuple2<Integer, Integer>>> joinData) throws Exception {
//				System.out.println(joinData._1);
//				System.out.println(joinData._2._1);
//				System.out.println(joinData._2._2);
//				System.out.println("==================================");
				// TODO Auto-generated method stub
				String[] fields = joinData._2._1.split("\t"); //用户评论信息
				double[] attributesValues = new double[4];
				attributesValues[0] = fields[7].split(":")[1].length(); // 用户留言的长度 应该是词向量 对其进行分词
				attributesValues[1] = joinData._2._2._1; // 用户评论主题总数
				attributesValues[2] = joinData._2._2._2; // 对用户有用的评论数
				attributesValues[3] = Double.parseDouble(fields[4].split(":")[1]); // 评分
				
				Vector v = Vectors.dense(attributesValues[2]/attributesValues[1],attributesValues);  //创建一个稠密向量
				//	org.apache.spark.mllib.linalg.Vector = (length,8.0,5.0,5.0);

				// LabeledPoint：
				// LabeledPoint数据格式是Spark自己定义的一种数据格式，他的原型是LIBSVM（台湾大学副教授开发的一种简单、易用和快速有效的SVM模式识别与回归的软件包）输入数据的格式类型。
				// LabeledPoint是一种标签数据，数据结构分为label
				// 和features两部分。具体结构为，label index1:value1 index2:value2
				// ...，其中label为标签数据，index1，index2为特征值序号，value1，value2为特征值。
				if((Double.parseDouble(fields[3].split(":")[1].split("/")[0]) / Double.parseDouble(fields[3].split(":")[1].split("/")[1]) ) >= 0.9) {
					return new LabeledPoint(1.0, v); // 标签,特征 // 有效评论
				} else {
					return new LabeledPoint(0.0, v); // 无效评论
				}
			}
		});
		
		/**
		 * 创建DataFrame 筛选处理后的数据 用于训练模型和测试
		 */
		DataFrame schemaReviews = sqlContext.createDataFrame(trainingRDD, LabeledPoint.class).cache();
				
				
		// Display 5 example rows.
		schemaReviews.show(50);


		// Split the data into training and test sets (30% held out for testing)
		DataFrame[] splits = schemaReviews.randomSplit(new double[]{0.7, 0.3}); // 在0.7和0.3的权重比例划分两个样本
		DataFrame trainingData = splits[0]; // 训练集
		DataFrame testData = splits[1];			// 测试集

		//LOGISTIC REGRESSION 逻辑回归
		LogisticRegression lr = new LogisticRegression();
		lr.setMaxIter(10);
		lr.setRegParam(0.01);
		
		//CLASSIFICATION TREE 决策树
		// StringIndexer按label出现的频次,转换成0～num numOfLabels-1(分类个数),频次最高的转换为0,以此类推.
		// StringIndexer将一列labels转译成[0,labels基数)的index，labels基数即为labels的去重后总量，index的顺序为labels频次升序，因此出现最多次labels的index为0。
		StringIndexerModel labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(trainingData);
		DecisionTreeClassifier dc = new DecisionTreeClassifier();
		dc.setImpurity("gini"); // // 不纯度
		dc.setLabelCol("indexedLabel");
		// IndexToString将index映射回原先的labels。
		IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedValue").setLabels(labelIndexer.labels());

        //EX 2: CREATE THE PIPELINE THAT IS USED TO BUILD THE CLASSIFICATION MODEL 创建管道，用于建立分类模型.
		// 可以使得多个机器学习算法顺序执行，达到高效的数据处理的目的.将多个Transformer和Estimator串成一个特定的ML Wolkflow.
		Pipeline pipelineLR =  new Pipeline().setStages(new PipelineStage[] { lr });
		Pipeline pipelineDC = new Pipeline().setStages(new PipelineStage[] { labelIndexer, dc, labelConverter });

		// Train model. Use the training set 训练模型。使用训练集
		PipelineModel modelLR = pipelineLR.fit(trainingData);
		PipelineModel modelDC = pipelineDC.fit(trainingData);
			
				
		/*==== EVALUATION评价 ====*/

		// Make predictions for the test set.对测试集进行预测.
		DataFrame predictionsLR = modelLR.transform(testData);
		DataFrame predictionsDC = modelDC.transform(testData);

		// Select example rows to display.
		predictionsLR.show(50);
		predictionsDC.show(50);

		// Retrieve the quality metrics.  检索质量度量.
        MulticlassMetrics metricsDC = new MulticlassMetrics(predictionsDC.select("prediction", "indexedLabel")); // 多类分类评估
        // Use the following command if you are using logistic regression 如果使用逻辑回归，请使用以下命令
        MulticlassMetrics metricsLR = new MulticlassMetrics(predictionsLR.select("prediction", "label")); // 多类分类评估

	    // Confusion matrix 混淆矩阵
        Matrix confusionLR = metricsLR.confusionMatrix();
        Matrix confusionDC = metricsDC.confusionMatrix();
        System.out.println("Confusion matrix LR 逻辑回归的混淆矩阵: \n" + confusionLR);
        System.out.println("Confusion matrix DC 决策树的混淆矩阵: \n" + confusionDC);
        
        double precisionLR = metricsLR.precision(); // 准确率(精度)
        double precisionDC = metricsDC.precision(); //  准确率(精度)
		System.out.println("Precision LR 逻辑回归准确率 = " + precisionLR);
		System.out.println("Precision DC 决策树准确率 = " + precisionDC);
		// Precision LR 逻辑回归准确率 = 0.8748400134725497
		// Precision DC 决策树准确率 = 0.8751364095655103 
		
	    // Close the Spark context
		sc.close();
	}
}
