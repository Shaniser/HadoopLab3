package ru.bmstu.hadoop.lab3;

public class FlightInfo {
    private float maxDelay;
    private int flightCount;
    private int cancelledCount;
    private int delayedCount;

    public FlightInfo(float maxDelay, int flightCount, int cancelledCount, int delayedCount) {
        this.maxDelay = maxDelay;
        this.flightCount = flightCount;
        this.cancelledCount = cancelledCount;
        this.delayedCount = delayedCount;
    }

    public float getMaxDelay() {
        return maxDelay;
    }

    public int getCancelledCount() {
        return cancelledCount;
    }

    public int getDelayedCount() {
        return delayedCount;
    }

    public int getFlightCount() {
        return flightCount;
    }

    public static FlightInfo add(FlightInfo info, float maxDelay, int cancelledCount, int delayedCount) {
        return new FlightInfo(Math.max(info.maxDelay, maxDelay),
                info.flightCount + 1,
                info.cancelledCount + cancelledCount,
                info.delayedCount + delayedCount);
    }


}
