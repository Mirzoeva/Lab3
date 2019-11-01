import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


public class Main {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flightsLines = sc.textFile("664600583_T_ONTIME_sample.csv");

        JavaRDD<String[]> flightsLinesParsed = flightsLines
                .map(new FlightData flight = FlightData)
                .filter(cols -> isNotColumnName(cols, FLIGHT_DEST_AIRPORT_INDEX, FLIGHT_DEST_AIRPORT_COLUMN_NAME));

    }
}
