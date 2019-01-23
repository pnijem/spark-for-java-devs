package com.pnijem.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class MainMapping {

    public static void main(String[] args){
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<Integer> inputData = new ArrayList<>();
        JavaSparkContext sc = null;
        try {
            inputData.add(35);
            inputData.add(40);
            inputData.add(90);
            inputData.add(20);
            SparkConf conf = new SparkConf().setAppName("sparkMapping").setMaster("local[*]");
            sc = new JavaSparkContext(conf);

            //JavaRDD allows us to communicate with our RDD using regular Java method. Under the hood, it is communicating with a Scala RDD

            JavaRDD<Integer> myRdd = sc.parallelize(inputData);//load in the collection and convert it to RDD

            Integer result = myRdd.reduce((val1, val2) -> val1 + val2);
            System.out.println(result);

            //RDD is immutable. Once it is created, it cannot be changed.
            // Thus, the map() will not change the RDD it is working on, it will create a new RDD.
            JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);

            //Do not call foreach() method directly on the RDD. Otherwise, you might get NotSerializableException error.
            //Instead, invoke collect().forEach() on the RDD.
            sqrtRdd.collect().forEach(System.out::println);

            //count the numbers of elements in the RDD
            Long elementsNum = myRdd.map(value -> 1L).reduce((val1, val2) -> val1 + val2);
            System.out.println(elementsNum);

        } catch(Exception e) {
            Logger.getLogger(MainMapping.class).error(e);
        } finally {
            if(sc!=null) {
                sc.close();
            }
        }

    }
}
