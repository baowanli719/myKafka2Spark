package com.gszq.bwl;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CartesianExample {
    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "D:\\myconf\\krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        SparkConf conf = new SparkConf().setAppName("Cartesian").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> javaRDD1 = jsc.parallelize(Arrays.asList(6, 7, 8, 9, 10));
        JavaPairRDD<Integer, Integer> cartesianRDD = javaRDD.cartesian(javaRDD1);
        cartesianRDD.foreach(x->System.out.println(x));

        jsc.stop();
    }
}