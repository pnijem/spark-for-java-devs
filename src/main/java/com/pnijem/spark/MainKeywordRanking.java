package com.pnijem.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MainKeywordRanking {

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
            //replace all characters which are not letters, with nothing
            JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() );

            //remove blank lines
            JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0 );

            JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split("\\s+")).iterator());
            JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

            JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(Util::isNotBoring);

            JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));

            JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
            //switch between keys and values
            JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<> (tuple._2, tuple._1 ));
            //sort results by key. false ==> descending order
            JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

            //take first 10 results
            List<Tuple2<Long,String>> results = sorted.take(10);
            results.forEach(System.out::println);
        } catch (Exception e) {
            Logger.getLogger(MainMapping.class).error(e);
        } finally {
            if (sc != null)
                sc.close();
        }
    }
}
