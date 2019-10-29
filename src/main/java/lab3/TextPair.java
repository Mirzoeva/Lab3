package lab2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair>{
    private Text key;
    private Text value;

    public TextPair(){
        this.key =new Text();
        this.value =new Text();
    }

    public TextPair(Text key, Text value) {
        this.key = key;
        this.value = value;
    }
    public TextPair(String key, String value){
        this.key =new Text(key);
        this.value =new Text(value);
    }

    public Text getKey() {
        return key;
    }

    public Text getValue() {
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        value.write(out);
    }

    @Override
    public int compareTo(TextPair o) {
        int difference = key.compareTo(o.getKey());
        if (difference == 0){
            return value.compareTo(o.getValue());
        }
        return difference;
    }

    @Override
    public String toString() {
        return key.toString() + "_" + value.toString();
    }
}