
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



}

