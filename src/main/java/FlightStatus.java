
import java.io.Serializable;

public class FlightStatus implements Serializable{
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

    public FlightStatus(String delay, String cancelled){
        if (delay.equals("")) {
            this.maxDelay = 0.f;
            this.delayedFlights = 0;
        }else {
            this.maxDelay = Float.parseFloat(delay);
            this.delayedFlights = (maxDelay > 0 ? 1 : 0);
        }
        this.cancelledFlights = (Float.parseFloat(cancelled) > 0 ? 1 : 0);
        this.flights = 1;
    }

    static FlightStatus add(FlightStatus a, FlightStatus b){
        return new FlightStatus(
                Math.max(a.getMaxDelay(), b.getMaxDelay()),
    }



}

