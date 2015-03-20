package org.apache.flink.streaming.examples.sampling;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.util.IterableIterator;

import java.io.Serializable;

/**
 * Created by marthavk on 2015-03-18.
 */
public class Distribution implements Serializable{

    double mean;
    double sigma;

    public Distribution() {

    }

    public Distribution(double cMean, double cSigma) {
        this.mean = cMean;
        this.sigma = cSigma;
    }

    public Distribution(Reservoir<Double> reservoir) {
        SummaryStatistics stats = new SummaryStatistics();
        for (Double value : reservoir.getReservoir()) {
            stats.addValue(value);
        }

        this.mean = stats.getMean();
        this.sigma = stats.getStandardDeviation();

    }



    public double getMean() {
        return mean;
    }

    public double getSigma() {
        return sigma;
    }



/*    public void setMean(double mean) {
        this.mean = mean;
    }

    public void setSigma(double sigma) {
        this.sigma = sigma;
    }*/

    public void updateMean(long count, double mStep, int interval) {
        this.mean += (count%interval == 0 ? mStep : 0);
    }

    public void updateSigma(long count, double sStep, int interval) {
        this.sigma += (count%interval == 0 ? sStep : 0);
    }

    @Override
    public String toString() {
        return "[" + this.mean + ", " + this.sigma + "]";
    }

}
