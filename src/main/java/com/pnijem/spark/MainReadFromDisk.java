package com.pnijem.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class MainReadFromDisk {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {
            System.setProperty("hadoop.home.dir", "c:/hadoop");
            Logger.getLogger("org.apache").setLevel(Level.WARN);

            SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            sc = new JavaSparkContext(conf);
            //load a file from disk
            JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
            //print out all the words in this file
            initialRdd.flatMap((value -> Arrays.asList(value.split("\\s+")).iterator()))
                    .collect().forEach(System.out::println);

        } catch (Exception e) {
            Logger.getLogger(MainMapping.class).error(e);
        } finally {
            if (sc != null)
                sc.close();
        }
    }
}
