package lab2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ComparatorAirports extends WritableComparator {
    public ComparatorAirports(){
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b){
        TextPair a1 = (TextPair)a;
        TextPair b1 = (TextPair)b;
        return a1.getKey().compareTo(b1.getKey());
    }
}
