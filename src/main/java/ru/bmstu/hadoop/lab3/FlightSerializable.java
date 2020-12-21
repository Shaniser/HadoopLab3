package ru.bmstu.hadoop.lab3;

import scala.Serializable;

public class FlightSerializable implements Serializable {
    private boolean isCanceled;
    private float delayTime;

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
