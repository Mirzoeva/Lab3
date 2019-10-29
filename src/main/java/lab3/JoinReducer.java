package lab2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        int count = 0;
        float maxTime = -1;
        float minTime = Integer.MAX_VALUE;
        float sum = 0;
        Text airportName = new Text(iter.next().toString() + ",");
        while (iter.hasNext()) {
            float currentTime = Float.parseFloat(iter.next().toString());
            maxTime = Math.max(currentTime, maxTime);
            minTime = Math.min(currentTime, minTime);
            sum += currentTime;
            count++;
        }
        if (count != 0) {
            float average = sum / count;
            context.write(airportName, new Text("Min: " + minTime + ", Max: " + maxTime + ", Average: " + average));
        }
    }
}
