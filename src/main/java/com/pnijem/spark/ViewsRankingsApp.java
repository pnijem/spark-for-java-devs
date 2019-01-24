package com.pnijem.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewsRankingsApp {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {
            System.setProperty("hadoop.home.dir", "c:/hadoop");
            Logger.getLogger("org.apache").setLevel(Level.WARN);

            SparkConf conf = new SparkConf().setAppName("videoRankingByViewNumber").setMaster("local[*]");
            sc = new JavaSparkContext(conf);

            // Use true to use hardcoded data. false means the hardcoded data will be ignored and the relevant files will be loaded
            boolean testMode = false;

            JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
            JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
            JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

            //An RDD containing a key of courseId together with the number of chapters on that course
            JavaPairRDD<Integer, Integer> chapterCountRDD = chapterData.mapToPair(chapter -> new Tuple2<>(chapter._2, 1))
                    .reduceByKey((value1, value2) -> value1 + value2);

            //remove any duplicated views
            viewData = viewData.distinct();

            JavaPairRDD<Integer, Integer> viewDataSwitched = viewData.mapToPair(row -> new Tuple2<>(row._2, row._1));
            JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRDD = viewDataSwitched.join(chapterData);
            // We Don't need chapterIds, setting up for a reduce
            JavaPairRDD<Tuple2<Integer, Integer>, Long> joinedRDDSwitched = joinedRDD.mapToPair(row -> {
                Integer userId = row._2._1;
                Integer courseId = row._2._2;
                return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
            });

            joinedRDDSwitched = joinedRDDSwitched.reduceByKey((val1, val2) -> val1 + val2);

            //remove the courseId. We need to have only two columns;courseId and views
            JavaPairRDD<Integer, Long> viewCountRdd =
                    joinedRDDSwitched.mapToPair(row -> new Tuple2<>(row._1._2, row._2));

            //add the total chapter count
            JavaPairRDD<Integer, Tuple2<Long, Integer>> viewsOfChaptersRdd = viewCountRdd.join(chapterCountRDD);

            //convert to percentage then to scores
            JavaPairRDD<Integer, Long> scoresRDD = viewsOfChaptersRdd.mapValues(value -> (double) value._1 / value._2)
                    .mapValues(value -> {
                        if (value > 0.9) return 10L; // If a user watches more than 90% of the course, the course gets 10 points
                        if (value > 0.5) return 4L; // If a user watches > 50% but <90% , it scores 4
                        if (value > 0.25) return 2L; // If a user watches > 25% but < 50% it scores 2
                        return 0L; //Less than 25% is no score
                    });

            scoresRDD = scoresRDD.reduceByKey((value1, value2) -> value1 + value2);

            JavaPairRDD<Long, String> resultRdd = scoresRDD.join(titlesData)
                    .mapToPair(row -> new Tuple2<>(row._2._1, row._2._2));

            resultRdd.sortByKey(false).collect().forEach(System.out::println);

        } catch (Exception e) {
            Logger.getLogger(ViewsRankingsApp.class).error(e);
        } finally {
            if (sc != null) {
                sc.close();
            }
        }

    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
