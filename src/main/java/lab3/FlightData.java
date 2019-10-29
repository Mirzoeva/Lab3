package lab2;

import org.apache.hadoop.io.Text;

public class FlightData {
    private String[] columns;
    private static final int ID_ROW = 14;
    private static final int DELAY_ROW = 18;

    public FlightData(String str){
        columns = ParserUtils.splitCommas(str);
    }

    public FlightData(Text str){
        columns = ParserUtils.splitCommas(str.toString());
    }

    public String getAirportID(){
        return columns[ID_ROW];
    }

    public String getDelay(){
        return columns[DELAY_ROW];
    }

}
