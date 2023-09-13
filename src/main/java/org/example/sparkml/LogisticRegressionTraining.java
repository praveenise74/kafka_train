package org.example.sparkml;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class LogisticRegressionTraining {
    static String diabetesURL = "C:\\Users\\Dell\\IdeaProjects\\trainl2\\kafka_train\\src\\main\\resources\\ml_dataset\\diabetes.csv";

    public static void main(String[] args) {
//        String winUtilPath = "C:\\softwares\\winutils";
//        if (System.getProperty("os.name").toLowerCase().contains("win")) {
//            System.out.println("detected windows");
//            System.setProperty("hadoop.home.dir", winUtilPath);
//            System.setProperty("HADOOP_HOME", winUtilPath);
//        }

        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> rawDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(diabetesURL);

        Dataset<Row> requiredFields = rawDf.na().drop();

        Dataset<Row> fieldsForTraining = requiredFields.selectExpr(
                        "cast(Pregnancies as double)", "cast(Glucose as double)",
                        "cast(BloodPressure as double)", "cast(SkinThickness as double)",
                        "cast(Insulin as double)",
                        "cast(BMI as double)", "cast(DiabetesPedigreeFunction as double)",
                        "cast(Age as double)", "cast(Outcome as double)")
                .withColumnRenamed("Outcome", "label");

        fieldsForTraining.show();
        fieldsForTraining.printSchema();
        String[] inputCols = new String[] {
                "Pregnancies","Glucose","BloodPressure","SkinThickness",
                "Insulin","BMI","DiabetesPedigreeFunction","Age"
        };

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(inputCols)
                .setOutputCol("features");

        Dataset<Row> finalDf = assembler.transform(fieldsForTraining);

        finalDf.show();
        finalDf.printSchema();
        Dataset<Row>[] trainTestDf = finalDf.randomSplit(new double[] { 0.8, 0.2});
        Dataset<Row> tainDf = trainTestDf[0];
        Dataset<Row> testDf = trainTestDf[1];

        LogisticRegression lr = new LogisticRegression();
        LogisticRegressionModel trainedModel = lr.train(tainDf);
        System.out.println(trainedModel.coefficients());
        Dataset<Row> testPredictionsDf = trainedModel.transform(testDf);
        testPredictionsDf.show();
        double evaluator = new BinaryClassificationEvaluator()
//.setMetricName("rmse")
                .evaluate(testPredictionsDf);
        System.out.println("accuracy = " + evaluator);

    }
}
