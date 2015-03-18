package org.apache.flink.streaming.examples.sampling;

import java.io.Serializable;

/**
 * Created by marthavk on 2015-03-18.
 */
public class Parameters implements Serializable {
    double mean0;
    double stDev0;
    int changeInterval;
    double mStep;
    double sStep;

    public Parameters(double c_mean, double c_stDev, int c_interval, double c_mrate, double c_srate) {
        this.mean0 = c_mean;
        this.stDev0 = c_stDev;
        this.changeInterval = c_interval;
        this.mStep = c_mrate;
        this.sStep = c_srate;
    }

    public Double getCurrentMean(Long count) {
        return mean0 + (count/changeInterval)*mStep;
    }

    public double getCurrentStDev(long count) {
        double currentStDev = stDev0 + (count/changeInterval)*sStep;
        if (currentStDev >= 0) {
            return currentStDev;
        }
        else {
            return 0;
        }
    }


}
