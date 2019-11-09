package lab3;

import org.apache.hadoop.io.Text;

public class ParserDelays {
    private String[] mainString;
    private static final int ID_ROW = 14;
    private static final int DELAY_ROW = 18;
    private static final int FLIGHT_AIRPORT_INDEX = 11;

    public ParserDelays(String[] str){
        mainString = str;
    }


    public String getAirportID(){
        return mainString[ID_ROW];
    }

    public String getValue(){
        return mainString[DELAY_ROW];
    }

    public String getAirportIndex(){
        return mainString[FLIGHT_AIRPORT_INDEX];
    }


}