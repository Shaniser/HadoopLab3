package ru.bmstu.hadoop.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

/*
Start project
*start hadoop*
mvn package
hadoop fs -copyFromLocal Airports.csv Flights.csv /
spark-submit --class ru.bmstu.hadoop.lab3.AirportFindDelay --master yarn-client --num-executors 3 /Users/shaniser/Desktop/Hadoop/HadoopLab3/target/spark-examples-1.0-SNAPSHOT.jar
hadoop fs -copyToLocal output
*/

public class AirportFindDelay {
    private static final String FLIGHTS_FIRST_STRING = "\"YEAR\"";
    private static final String AIRPORTS_FIRST_STRING = "Code";
    public static final int DELAY_COLUMN = 18;
    public static final int CANCEL_CODE_INDEX = 19;
    public static final int ERROR_CODE = 1;
    public static final int AIRPORT_ID_COLUMN = 0;
    public static final int AIRPORT_NAME_COLUMN = 1;
    public static final int AIRPORT_DELAY_COLUMN_ID = 14;
    public static final float CANCEL_CODE = 1;
    public static final int CANCEL_CODE_COLUMN = 19;
    public static final float ZERO_TIME = 0;
    public static float maxPercent = 100;

    public static final String EMPTY_STR = "";

    // Regexes
    public static final String REGEX_QUOTES = "^\"+|\"+$";
    public static final String REGEX_CVS_SPLIT = ",";

    // Strings
    public static final String MAX_DELAY_STR = "Max delay is ";
    public static final String FLIGHTS_STR = "Flights count is ";
    public static final String FLIGHTS_CANCELLED_STR = "Cancelled flights count is ";
    public static final String FLIGHTS_DELAYED_STR = "Delayed flights count is ";
    public static final String DELAYED_PERCENT_STR = "Delayed flights percent is ";
    public static final String CANCELLED_PERCENT_STR = "Cancelled flights percent is ";
    public static final String FROM_STR = "From: ";
    public static final String TO_STR = " to: ";
    public static final String NEW_STR = " \n";

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

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("/Airports.csv");
        JavaRDD<String> flights = sc.textFile("/Flights.csv");

        JavaPairRDD<Integer, String> airportInfo = airports
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

                    boolean isCanceled = isCanceled(getCancelCode(values[CANCEL_CODE_COLUMN]));
                    float delayTime = Float.parseFloat(values[DELAY_COLUMN]);

                    FlightSerializable flightDelayInfo = new FlightSerializable(delayTime, isCanceled);
                    return new Tuple2<>(new Tuple2<>(destinationAirportId, originalAirportId), flightDelayInfo);
                });


        JavaPairRDD<Tuple2<Integer, Integer>, FlightInfo> flightCombined =
                flightInfo.combineByKey(
                        value -> new FlightInfo(
                                value.getDelayTime(),
                                1,
                                value.isCanceled() ? 1 : 0,
                                value.getDelayTime() > ZERO_TIME ? 1 : 0
                        ),
                        (flight, value) -> FlightInfo.add(flight,
                                value.getDelayTime(),
                                value.isCanceled() ? 1 : 0,
                                value.getDelayTime() > ZERO_TIME ? 1 : 0
                        ),
                        FlightInfo::add
                );

        JavaPairRDD<Tuple2<Integer, Integer>, String> flightInfoStr =
                flightCombined.mapToPair(
                        value -> {
                            float cancelledPercent = 0;
                            float delayedPercent = 0;

                            if (value._2.getDelayedCount() != 0) {
                                delayedPercent = (float) value._2.getDelayedCount() / value._2.getFlightCount() * maxPercent;
                            }

                            if (value._2.getCancelledCount() != 0) {
                                cancelledPercent = (float) value._2.getCancelledCount() / value._2.getFlightCount() * maxPercent;
                            }

                            String infoStrBuilder = MAX_DELAY_STR +
                                    value._2.getMaxDelay() +
                                    FLIGHTS_STR +
                                    value._2.getFlightCount() +
                                    FLIGHTS_CANCELLED_STR +
                                    value._2.getCancelledCount() +
                                    FLIGHTS_DELAYED_STR +
                                    value._2.getMaxDelay() +
                                    DELAYED_PERCENT_STR +
                                    delayedPercent +
                                    CANCELLED_PERCENT_STR +
                                    cancelledPercent;
                            return new Tuple2<>(value._1, infoStrBuilder);
                        }
                );

        final Broadcast<Map<Integer, String>> airportBroadcast = sc.broadcast(airportInfo.collectAsMap());

        JavaRDD<String> outputInfo = flightInfoStr.map(
                value -> {
                    Map<Integer, String> name = airportBroadcast.getValue();

                    String from = name.get(value._1._1);
                    String to = name.get(value._1._2);

                    return FROM_STR + from + TO_STR + to + NEW_STR + value._2;
                }
        );

        airportInfo.saveAsTextFile("output");
    }
}
