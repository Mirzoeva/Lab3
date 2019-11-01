import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


public class Main {
    private static final int ID_ROW = 14;
    private static final int DELAY_ROW = 18;

    private static boolean isNotColumnName(String[] cols, int columnIndex, String columnName) {
        return !cols[columnIndex].equals(columnName);
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flightsLines = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String[]> flightsLinesParsed = flightsLines
                .map(ParserUtils::splitAll)
                .filter(cols -> isNotColumnName(cols, ID_ROW, FLIGHT_DEST_AIRPORT_COLUMN_NAME));

    }
}
