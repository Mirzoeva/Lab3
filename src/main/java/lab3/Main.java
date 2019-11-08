package lab3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class Main {
    private static final int FLIGHT_ID_ROW = 14;
    private static final int FLIGHT_AIRPORT_INDEX = 11;
    private static final int DELAY_ROW = 18;
    private static final int AIRPORTS_ID_ROW = 0;
    private static final int NAME_AIRPORT_ROW = 1;
    private static final int FLIGHT_CANCELLED_INDEX = 19;

    private static boolean isColumnName(String[] cols, int colIndex, String colName){
        return !cols[colIndex].equals(colName);
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsLines = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String[]> flightsLinesParsed = flightsLines.
                map(ParserUtils::splitCommas).
                filter(cols -> isColumnName(cols, FLIGHT_ID_ROW, "DEST_AIRPORT_ID"));
        JavaPairRDD<Tuple2<String, String>, FlightData> flightStatPairs = flightsLinesParsed
                .mapToPair(
                        cols -> new Tuple2<>(
                                new Tuple2<>(cols[FLIGHT_AIRPORT_INDEX], cols[FLIGHT_ID_ROW]),
                                new FlightData(cols[DELAY_ROW], cols[FLIGHT_CANCELLED_INDEX])
                        )
                );
        JavaPairRDD<Tuple2<String, String>, FlightData> flightsStatPairsSummarized = flightStatPairs
                .reduceByKey(FlightData::add);

        JavaRDD<String> airportsLines = sc.textFile("L_AIRPORT_ID.csv");
        JavaRDD<String[]> airportsLineParsed = airportsLines
                .map(ParserUtils::splitAll)
                .filter(cols -> isColumnName(cols, AIRPORTS_ID_ROW, "Code"));
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");

        System.out.println(airportsLineParsed);
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");
        System.out.println("\n");

        System.out.println("\n");

        JavaPairRDD<String, String> airportsPeirs = airportsLineParsed
                .mapToPair(cols -> new Tuple2<>(cols[AIRPORTS_ID_ROW], cols[NAME_AIRPORT_ROW]));
//        Map<String, String> airportsMap = airportsPeirs.collectAsMap();
//        final Broadcast<Map<String,String> > airportsBroadcast = sc.broadcast(airportsMap);
//        JavaRDD<String> statusLines = flightsStatPairsSummarized.map(
//                pair -> airportsBroadcast.value().get(pair._1._1) + ", "
//                        + airportsBroadcast.value().get(pair._1._2) + ", "
//                        + pair._2.toString());
//        statusLines.saveAsTextFile("output");
    }

}
