package ru.bmstu.hadoop.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
Start project
*start hadoop*
mvn package
hadoop fs -copyFromLocal Airports.csv Flights.csv /
spark-submit --class ru.bmstu.hadoop.lab3.AirportFindDelay --master yarn-client --num-executors 3 /Users/shaniser/Desktop/Hadoop/HadoopLab3/target/spark-examples-1.0-SNAPSHOT.jar
hadoop fs -copyToLocal output
*/

public class AirportFindDelay {
    private static final String FIRST_STRING = "\"YEAR\"";
    public static final int DELAY_INDEX = 18;
    public static final int CANCEL_CODE_INDEX = 19;
    public static final int ID_INDEX = 14;
    public static final int ERROR_CODE = 1;

    public static final String EMPTY_STR = "";

    // Regexes
    public static final String REGEX_QUOTES = "^\"+|\"+$";
    public static final String REGEX_CVS_SPLIT = ",";

    private static boolean isFirstLine(String str) {
        return str.contains(FIRST_STRING);
    }

    private static String removeQuotes(String str) {
        return str.replaceAll(REGEX_QUOTES, EMPTY_STR);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("/Airports.csv");
        JavaRDD<String> flights = sc.textFile("/Flights.csv");

        JavaPairRDD<Integer, String> flightsStr = airports
                .filter(str -> !isFirstLine(str))
                .mapToPair(str -> {
                    String[] values = str.split(REGEX_CVS_SPLIT);
                    Integer
                })



        flightsStr.saveAsTextFile("output");
    }
}
