package lab3;

import org.apache.hadoop.io.Text;

public class FlightInfo {
    private String[] mainString;
    private static final int ID_ROW = 14;
    private static final int DELAY_ROW = 18;
    private static final int FLIGHT_AIRPORT_INDEX = 11;
    private static final int FLIGHT_CANCELLED_INDEX = 19;

    public FlightInfo(String[] str){
        mainString = str;
    }


    public String getAirportID(){
        return mainString[ID_ROW];
    }

    public String getDelayTime(){
        return mainString[DELAY_ROW];
    }

    public String getAirportSecondID(){
        return mainString[FLIGHT_AIRPORT_INDEX];
    }

    public String getCancelled(){
        return mainString[FLIGHT_CANCELLED_INDEX];
    }

}