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

    static FlightData add(FlightData a, FlightData b){
        return new FlightData(
                Math.max(a.getMaxDelay(), b.getMaxDelay()),
                a.getDelayedFlights() + b.delayedFlights,
                a.getCancelledFlights() + b.getCancelledFlights(),
                a.getFlights() + b.getFlights()
        );
    }

    @Override
    public String toString(){
        if (delayedFlights == 0){
            return "No Delays and Cancels";
        } else {
            return maxDelay + "," + (float) delayedFlights/flights *100f + "%, " +
                (float)cancelledFlights/flights*100f + "% " + "\n";
        }
    }

}

