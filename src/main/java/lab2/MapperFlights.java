package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperFlights extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable stringNumber, Text textIn, Context context) throws IOException, InterruptedException {
        if (stringNumber.get() == 0 || textIn.toString().isEmpty()) {
            return;
        }
        FlightData flightData = new FlightData(textIn);
        String timeString = flightData.getDelay();
        if (timeString.isEmpty()){
            return;
        }
        Text timeText = new Text(timeString);
        TextPair airportID = new TextPair(flightData.getAirportID(), "1");

        if (Float.parseFloat(timeString) > 0){
            context.write(airportID, timeText);
        }
    }
}
