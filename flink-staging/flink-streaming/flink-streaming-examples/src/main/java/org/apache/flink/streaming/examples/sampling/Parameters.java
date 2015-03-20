package org.apache.flink.streaming.examples.sampling;

import java.io.Serializable;

/**
 * Created by marthavk on 2015-03-18.
 */
public class Parameters implements Serializable {
	double meanInit;
	double sigmaInit;
	double mStep;
	double sStep;
	int interval;
	int sampleSize;

	public Parameters(double c_mean, double c_sigma, double c_mrate, double c_srate, int c_interval, int c_size) {
		this.meanInit = c_mean;
		this.sigmaInit = c_sigma;
		this.interval = c_interval;
		this.mStep = c_mrate;
		this.sStep = c_srate;
		this.sampleSize = c_size;
	}

	public double getMeanInit() {
		return meanInit;
	}

	public double getSigmaInit() {
		return sigmaInit;
	}

	public double getmStep() {
		return mStep;
	}

	public double getsStep() {
		return sStep;
	}

	public int getInterval() {
		return interval;
	}

	public int getSampleSize() {
		return sampleSize;
	}

    /*public void setMean(Long count) {

        this.mean0 += (count%changeInterval == 0 ? mStep : 0);
    }

    public void setStDev(Long count) {
        this.stDev0 += (count%changeInterval == 0 ? sStep : 0);
    }

    public double getMean() {
        return this.mean0;
    }

    public double getStDev() {
        return this.stDev0;
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
    }*/


}
