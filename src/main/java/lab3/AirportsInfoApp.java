package lab3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class AirportsInfoApp {
    private static final String AIRPORTS_FILE_PATH = "L_AIRPORT_ID.csv";
    private static final int AIRPORTS_AIRPORTS_ID = 0;
    private static final String Code = "Code";

    private static final String FLIGHTS_FILE_PATH = "664600583_T_ONTIME_sample.csv";
    private static final int FLIGHT_ID = 14;
    private static final String DEST_AIRPORT_ID = "DEST_AIRPORT_ID";


    private static boolean isNotEqualName(String[] cols, int colIndex, String colName){
        return !cols[colIndex].equals(colName);
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsLines = sc.textFile(FLIGHTS_FILE_PATH);
        JavaRDD<String> airportsLines = sc.textFile(AIRPORTS_FILE_PATH);


        JavaRDD<String[]> flightsLinesParsed = flightsLines
                .map(ParserUtils::splitAll)
                .filter(cols -> isNotEqualName(cols, FLIGHT_ID, DEST_AIRPORT_ID));

        JavaPairRDD<Tuple2, FlightData> flightStatPairs = flightsLinesParsed
                .mapToPair(
                        cols -> {
                            FlightInfo flightData = new FlightInfo(cols);
                            return new Tuple2<>(
                                new Tuple2<>(flightData.getAirport(), flightData.getAirportID()),
                                new FlightData(flightData.getDelayTime(),flightData.getCancelled()));
                        }
                );

        JavaPairRDD<Tuple2, FlightData> flightsStatPairsSummarized = flightStatPairs
                .reduceByKey(FlightData::addFlightData);


        JavaRDD<String[]> airportsLineParsed = airportsLines
                .map(ParserUtils::splitAll)
                .filter(cols -> isNotEqualName(cols, AIRPORTS_AIRPORTS_ID, Code));

        Map<String, String> stringAirportDataMap = airportsLineParsed
                .mapToPair(cols -> {
                    AirportsInfo airportInfo = new AirportsInfo(cols);
                    return new Tuple2<>(airportInfo.getAirportID(), airportInfo.getAirportName());
                })
                .collectAsMap();


        final Broadcast<Map<String,String> > airportsBroadcast = sc.broadcast(stringAirportDataMap);


        JavaRDD<String> statusLines = flightsStatPairsSummarized.map(
                pair -> airportsBroadcast.value().get(pair._1._1) + ", "
                        + airportsBroadcast.value().get(pair._1._2) + ", "
                        + pair._2.toString());

        statusLines.saveAsTextFile("output");
    }
}
