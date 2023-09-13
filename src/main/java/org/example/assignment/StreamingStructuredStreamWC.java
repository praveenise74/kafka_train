package org.example.assignment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class StreamingStructuredStreamWC {

    public static void main(String[] args) throws TimeoutException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SocgenJavaStructuredStreaming")
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", "2");
        spark.sparkContext().setLogLevel("WARN");

        StructType returnSchema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("ID", DataTypes.IntegerType, false),
                        DataTypes.createStructField("wordInput", DataTypes.StringType, false)
                });

        Dataset<Row> positiveWordType = spark.read()
                .option("header", "false")
                .schema(returnSchema)
                .csv("file:///C:\\Users\\Dell\\IdeaProjects\\trainl2\\kafka_train\\data\\static\\positive words.csv");

//        positiveWordType.show(false);

        Dataset<Row> negativeWordType = spark.read()
                .option("header", "false")
                .schema(returnSchema)
                .csv("file:///C:\\Users\\Dell\\IdeaProjects\\trainl2\\kafka_train\\data\\static\\negative words.csv");

//        negativeWordType.show(false);

        Dataset<Row> streamingFiles = spark.readStream().text("file:///C:\\Users\\Dell\\IdeaProjects\\trainl2\\kafka_train\\data\\stream\\conversation");



        Dataset<Row> words = streamingFiles.select(functions.explode(
                        functions.split(streamingFiles.col("value"), " "))
                .alias("word"));


        Dataset<Row> joinedPositiveWords = words.join(positiveWordType,
                words.col("word").equalTo(positiveWordType.col("wordInput"))).withColumn("WORD_TYPE", functions.lit("POSITIVE"));

        Dataset<Row> joinedNegativeWordType = words.join(negativeWordType,
                words.col("word").equalTo(negativeWordType.col("wordInput"))).withColumn("WORD_TYPE", functions.lit("NEGATIVE"));

        Dataset<Row>  result =  joinedPositiveWords.union(joinedNegativeWordType);
        result = result.groupBy("WORD_TYPE").count();

//        String documnetType = result.first().getString(0);

//        result = result.withColumn("DocumnetType", functions.lit(result.first().getString(0)));

//        System.out.println("documnetType:"+ documnetType);

        StreamingQuery query1 = result.writeStream()
                .outputMode("complete")
//                .outputMode("append")
                .foreachBatch((batchDf, batchID) -> {
                    String documentType = batchDf.head().getString(0);
                    System.out.println("documentType"+documentType);
//                    batchDf.withColumn("documentType", functions.lit(documentType)).show(false);
                })
                .format("console").start();
        try {
            query1.awaitTermination(1000000);
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

        System.out.println("above was stream output");


//        result =  result.withColumn("TEMPCOl", result.col("count"))
//                .orderBy(functions.desc("TEMPCOl"))
//                .withColumn("Document_type" , functions.col("WORD_TYPE"));


        //      result =  result.withColumn("DOCUMENT_TYPE" , result.first().getAs("TEMPCOl") )
        //.first().getAs("TEMPCOl");
//                .withColumn("documentType",functions.when(
//                        postiveWordCount.col("count").$less(NegativeWordCount.col("count"))
//                        ,functions.lit("POSITIVE_DOCUMENT")).otherwise(functions.lit("Negative_DOCUMENT")))
        ;
//        StreamingQuery query = result.writeStream()
//                .outputMode("complete")
//                //.outputMode("append")
//                .format("console").start();
//        try {
//            query.awaitTermination(1000);
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }
    }
}
