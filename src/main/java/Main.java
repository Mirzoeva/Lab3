import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class Main {
    private static final int ID_ROW_FOR_FLIGHT = 14;
    private static final int FLIGHT_ORIGIN_AIRPORT_INDEX = 11;
    private static final int DELAY_ROW = 18;
    private static final int ID_ROW_FOR_AIRPORTS = 0;
    private static final int NAME_AIRPORT_ROW = 1;
    private static final int FLIGHT_CANCELLED_INDEX = 19;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsLines = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String[]> flightsLinesParsed = flightsLines.map(ParserUtils::splitAll);
        JavaPairRDD<Tuple2<String, String>, FlightStatus> flightStatPairs = flightsLinesParsed
                .mapToPair(
                        cols -> new Tuple2<>(
                                new Tuple2<>(cols[FLIGHT_ORIGIN_AIRPORT_INDEX], cols[ID_ROW_FOR_FLIGHT]),
                                new FlightStatus(cols[DELAY_ROW], cols[FLIGHT_CANCELLED_INDEX])
                        )
                );
        JavaPairRDD<Tuple2<String, String>, FlightStatus> flightsStatPairsSummarized = flightStatPairs
                .reduceByKey(FlightStatus::add);

        JavaRDD<String> airportsLines = sc.textFile("L_AIRPORT_ID.csv");
        JavaRDD<String[]> airportsLineParsed = airportsLines.map(ParserUtils::splitCommas);
        JavaPairRDD<String, String> airportsPeirs = airportsLineParsed.mapToPair(cols -> new Tuple2<>(cols[ID_ROW_FOR_AIRPORTS], cols[NAME_AIRPORT_ROW]));
        Map<String, String> airportsMap = airportsPeirs.collectAsMap();
        final Broadcast<Map<String,String> > airportsBroadcast = sc.broadcast(airportsMap);
        JavaRDD<String> statusLines = flightsStatPairsSummarized.map(pair -> airportsBroadcast.value().get(pair._1._1) + ", " + airportsBroadcast.value().get(pair._1._2) + ", " + pair._2.toString());
        statusLines.saveAsTextFile(args[0]);
    }

}
