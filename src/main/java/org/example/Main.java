package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textFile = javaSparkContext.textFile("dataInput.txt");
/**Splitting the word with space*/
        textFile = textFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
/**Pair the word with count*/
        JavaPairRDD<String, Integer> mapToPair = textFile.mapToPair(w -> new Tuple2<>(w, 1));
/**Reduce the pair with key and add count*/
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey((Integer::sum));
        reduceByKey.saveAsTextFile("word-count-output.txt");
        javaSparkContext.close();
    }
}