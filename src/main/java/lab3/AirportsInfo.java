package lab3;

import org.apache.hadoop.io.Text;

public class AirportsInfo {
    private String[] mainString;
    private static final int ID_ROW = 0;
    private static final int NAME_ROW = 1;

    public AirportsInfo(String[] str){
        mainString = str;
    }

    public String getAirportFirstID(){
        return mainString[ID_ROW];
    }

    public String getAirportName(){
        return mainString[NAME_ROW];
    }

}