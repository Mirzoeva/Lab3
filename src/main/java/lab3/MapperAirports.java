package lab2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class MapperAirports extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable stringNumber, Text textIn, Context context) throws IOException, InterruptedException {
        if (stringNumber.get() == 0 || textIn.toString().isEmpty()) {
            return;
        }
        AirportsData airportsData = new AirportsData(textIn);
        TextPair airportID = new TextPair(airportsData.getAirportID(), "0");
        Text airportName = new Text(airportsData.getAirportName());
        context.write(airportID, airportName);
    }
}
