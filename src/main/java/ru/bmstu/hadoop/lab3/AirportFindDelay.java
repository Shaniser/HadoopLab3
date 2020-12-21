package ru.bmstu.hadoop.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportFindDelay {
    SparkConf conf = new SparkConf().setAppName("lab3");
    JavaSparkContext sc = new JavaSparkContext(conf);

    
}
