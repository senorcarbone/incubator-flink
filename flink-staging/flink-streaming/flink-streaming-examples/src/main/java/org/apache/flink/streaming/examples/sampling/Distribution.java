package org.apache.flink.streaming.examples.sampling;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.util.IterableIterator;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-18.
 */

public class Distribution implements Serializable, NormalDistribution {

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


	@Override
	public double getMean() {
		return mean;
	}

	@Override
	public void setMean(double mean) {
		this.mean = mean;
	}

	@Override
	public double getStandardDeviation() {
		return sigma;
	}

	@Override
	public void setStandardDeviation(double s) {
		this.sigma = s;
	}

	@Override
	public double density(Double aDouble) {
		return 0;
	}


	public void updateMean(long count, double mStep, int interval) {
		this.mean += (count % interval == 0 ? mStep : 0);
	}

	public void updateSigma(long count, double sStep, int interval) {
		this.sigma += (count % interval == 0 ? sStep : 0);
	}

	@Override
	public String toString() {
		return "[" + this.mean + ", " + this.sigma + "]";
	}

	@Override
	public double inverseCumulativeProbability(double v) throws MathException {
		return 0;
	}

	@Override
	public double cumulativeProbability(double v) throws MathException {
		return 0;
	}

	@Override
	public double cumulativeProbability(double v, double v1) throws MathException {
		return 0;
	}

	public double nextGaussian() {
		return (new Random().nextGaussian() * sigma + mean);
	}
}
