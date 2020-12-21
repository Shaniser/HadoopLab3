package ru.bmstu.hadoop.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportFindDelay {
    private static final String FIRST_STRING = "\"YEAR\"";
    public static final int DELAY_INDEX = 18;
    public static final int CANCEL_CODE_INDEX = 19;
    public static final int ID_INDEX = 14;
    public static final int ERROR_CODE = 1;
    public static final String DELIMITER = ",";
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("Airports.csv");
        JavaRDD<String> flights = sc.textFile("Flights.csv");



    }
}
