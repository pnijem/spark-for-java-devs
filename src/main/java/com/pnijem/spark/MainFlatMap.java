package com.pnijem.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainFlatMap {
    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {

            List<String> inputData = new ArrayList<>();
            inputData.add("WARN: Tuesday 4 September 0405");
            inputData.add("ERROR: Tuesday 4 September 0408");
            inputData.add("FATAL: Wednesday 5 September 1632");
            inputData.add("ERROR: Friday 7 September 1854");
            inputData.add("WARN: Saturday 8 September 1942");

            Logger.getLogger("org.apache").setLevel(Level.WARN);

            SparkConf conf = new SparkConf().setAppName("sparkFlatMap").setMaster("local[*]");
            sc = new JavaSparkContext(conf);

            JavaRDD<String> sentences = sc.parallelize(inputData);

            //In flatMap() transformation, an element of source RDD can be mapped to one or more elements of target RDD.
            // A function is executed on every element of source RDD that produces one or more outputs.
            // The return type of the flatMap() function is java.util.iterator , that is, it returns a sequence of elements.
            JavaRDD<String> words = sentences.flatMap((value -> Arrays.asList(value.split("\\s+")).iterator()));
            words.collect().forEach(System.out::println);

            //filter words which their length is 1
            JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
            filteredWords.collect().forEach(System.out::println);

        } catch (Exception e) {
            Logger.getLogger(MainMapping.class).error(e);
        } finally {
            if (sc != null)
                sc.close();
        }
    }
}
