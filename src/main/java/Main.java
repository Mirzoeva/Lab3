import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;


public class Main {
    private static final int ID_ROW_FOR_FLIGHT = 14;
    private static final int FLIGHT_ORIGIN_AIRPORT_INDEX = 11;
    private static final int DELAY_ROW = 18;
    private static final int ID_ROW_FOR_AIRPORTS = 0;
    private static final int NAME_AIRPORT_ROW = 1;
    private static final int FLIGHT_CANCELLED_INDEX = 19;
    private static final String FLIGHT_DEST_AIRPORT_COLUMN_NAME = "DEST_AIRPORT_ID";

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

    }

}
