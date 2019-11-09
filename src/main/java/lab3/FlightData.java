package lab3;
import java.io.Serializable;

public class FlightData implements Serializable{
    private float maxDelay;
    private int delayedFlights;
    private int cancelledFlights;
    private int flights;

    public float getMaxDelay() {
        return maxDelay;
    }

    public int getDelayedFlights() {
        return delayedFlights;
    }

    public int getCancelledFlights() {
        return cancelledFlights;
    }

    public int getFlights() {
        return flights;
    }

    public FlightData(float maxDelay, int delayedFlights, int cancelledFlights, int flights){
        this.maxDelay = maxDelay;
        this.delayedFlights = delayedFlights;
        this.cancelledFlights = cancelledFlights;
        this.flights = flights;
    }

    public FlightData(String delay, String cancelled){
        this.cancelledFlights = (Float.parseFloat(cancelled) > 0 ? 1 : 0);
        if (delay.equals("")) {
            this.maxDelay = 0;
            this.delayedFlights = 0;
        } else {
            this.maxDelay = Float.parseFloat(delay);
            this.delayedFlights = (maxDelay > 0 ? 1 : 0);
        }
        this.flights = 1;
    }

    static FlightData addFlightData(FlightData airportPair1, FlightData airportPair2){
        return new FlightData(
                Math.max(airportPair1.getMaxDelay(), airportPair2.getMaxDelay()),
                airportPair1.getDelayedFlights() + airportPair2.getDelayedFlights(),
                airportPair1.getCancelledFlights() + airportPair2.getCancelledFlights(),
                airportPair1.getFlights() + airportPair2.getFlights()
        );
    }

    @Override
    public String toString(){
        if (delayedFlights == 0){
            return "No Delays and Cancels";
        } else {
            return "Max Delay: " + maxDelay + ", Delays Percent: " + this.getDelayPercent() + "%, Cancelled Percent: " +
                 this.getCancelPercent() + "% " + "\n";
        }
    }

    private float getDelayPercent(){
        return (float) delayedFlights/flights *100f;
    }

    private float getCancelPercent(){
        return (float)cancelledFlights/flights*100f;
    }

}

