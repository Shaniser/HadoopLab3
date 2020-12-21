package ru.bmstu.hadoop.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

/*
Start project
*start hadoop*
mvn package
hadoop fs -copyFromLocal Airports.csv Flights.csv /
spark-submit --class ru.bmstu.hadoop.lab3.AirportFindDelay --master yarn-client --num-executors 3 /Users/shaniser/Desktop/Hadoop/HadoopLab3/target/spark-examples-1.0-SNAPSHOT.jar
hadoop fs -copyToLocal output
*/

public class AirportFindDelay {
    private static final String AIRPORTS_FIRST_STRING = "\"YEAR\"";
    private static final String FLIGHTS_FIRST_STRING = "Code";
    public static final int DELAY_INDEX = 18;
    public static final int CANCEL_CODE_INDEX = 19;
    public static final int ERROR_CODE = 1;
    public static final int AIRPORT_ID_COLUMN = 0;
    public static final int AIRPORT_NAME_COLUMN = 1;
    public static final int AIRPORT_DELAY_COLUMN_ID = 14;
    public static final float CANCEL_CODE = 1;

    public static final String EMPTY_STR = "";

    // Regexes
    public static final String REGEX_QUOTES = "^\"+|\"+$";
    public static final String REGEX_CVS_SPLIT = ",";

    private static boolean isAirportsFirstLine(String str) {
        return str.contains(AIRPORTS_FIRST_STRING);
    }

    private static boolean isFlightsFirstLine(String str) {
        return str.contains(FLIGHTS_FIRST_STRING);
    }

    private static boolean isCanceled(float cancelled) {
        return cancelled == CANCEL_CODE;
    }

    private static String removeQuotes(String str) {
        return str.replaceAll(REGEX_QUOTES, EMPTY_STR);
    }

    private static String[] getValues(String str) {
        return str.split(REGEX_CVS_SPLIT);
    }

    private static float getCancelCode(String strCode) {
        return Float.parseFloat(strCode);
    }

    public static class FlightSerializable implements Serializable {
        private boolean isCanceled;
        private float delayTime;

        FlightSerializable(float delayTime, boolean isCanceled) {
            this.delayTime = delayTime;
            this.isCanceled = isCanceled;
        }

        boolean isCanceled() {
            return isCanceled;
        }

        public float getDelayTime() {
            return delayTime;
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("/Airports.csv");
        JavaRDD<String> flights = sc.textFile("/Flights.csv");

        JavaPairRDD<Integer, String> flightsStr = airports
                .filter(str -> !isAirportsFirstLine(str))
                .mapToPair(str -> {
                    String[] values = getValues(str);
                    Integer id = Integer.parseInt(removeQuotes(values[AIRPORT_ID_COLUMN]));
                    String name = values[AIRPORT_NAME_COLUMN];

                    return new Tuple2<>(id, name);
                });

        JavaPairRDD<Tuple2<Integer, Integer>, FlightSerializable> flightInfo = flights
                .filter(str -> !isFlightsFirstLine(str))
                .mapToPair(str -> {
                    String[] values = getValues(str);
                    Integer originalAirportId = Integer.parseInt(values[AIRPORT_DELAY_COLUMN_ID]);
                    Integer destinationAirportId = Integer.parseInt(values[AIRPORT_DELAY_COLUMN_ID]);

                    boolean isCanceled = isCanceled()
                });



        flightsStr.saveAsTextFile("output");
    }
}
