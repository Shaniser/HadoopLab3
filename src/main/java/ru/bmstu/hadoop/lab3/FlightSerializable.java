package ru.bmstu.hadoop.lab3;

import java.io.Serializable;

public class FlightSerializable implements Serializable {
    private final boolean isCanceled;
    private final float delayTime;

    FlightSerializable(float delayTime, boolean isCanceled) {
        this.delayTime = delayTime;
        this.isCanceled = isCanceled;
    }

    boolean isCanceled() {
        return isCanceled;
    }

    public float getDelayTime() {
        return delayTime;
    }
}
