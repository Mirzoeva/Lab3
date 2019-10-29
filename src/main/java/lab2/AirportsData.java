package lab2;

import org.apache.hadoop.io.Text;

public class AirportsData {
    private String[] columns;
    private static final int ID_ROW = 0;
    private static final int NAME_ROW = 1;

    public AirportsData(String str){ columns = ParserUtils.splitAll(str); }

    public AirportsData(Text str){
        columns = ParserUtils.splitAll(str.toString());
    }

    public String getAirportID(){
        return columns[ID_ROW];
    }

    public String getAirportName(){
        return columns[NAME_ROW];
    }

}