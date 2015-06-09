/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.sampling.generators;

import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by marthavk on 2015-05-07.
 */
public class GaussianDistribution implements Serializable, NumberGenerator<Double> {
	Random n = new Random();
	double mean;
	double sigma;
	double outlierRate;

	public GaussianDistribution() {
		super();
	}

	public GaussianDistribution(double mean, double sigma) {
		this.mean = mean;
		this.sigma = sigma;
		this.outlierRate = 0;
	}

	public GaussianDistribution(double mean, double sigma, double outlierRate) {
		this.mean = mean;
		this.sigma = sigma;
		this.outlierRate = outlierRate;
	}

	public double getMean() {
		return this.mean;
	}

	public double getStandardDeviation() {
		return this.sigma;
	}

	@Override
	public Double generate() {
		boolean success = SamplingUtils.flip(outlierRate);
		if (success) {
			return generateOutlier();
		} else {
			return n.nextGaussian() * sigma + mean;
		}
	}

	@Override
	public String toString() {
		return "[" + mean + "," + sigma + "]";
	}


	/**
	 * an outlier is generated using the 1.5xIQR rule uniformly at random
	 * inside the interval: [q1-2*IQR, q1-1.5IQR)U[q3+1.5*IQR, q3+2*IQR)
	 *
	 * @return
	 */
	public double generateOutlier() {
		double q1 = inverseCumulativeProbability(0.25);
		double q3 = inverseCumulativeProbability(0.75);
		double iqr = q3 - q1;
		double outlier;
		if (SamplingUtils.flip(0.5)) {
			outlier = q3 + 1.5 * iqr + Math.random() * sigma;
		} else {
			outlier = q1 - 1.5 * iqr - Math.random() * sigma;
		}
		return outlier;
	}

	public double inverseCumulativeProbability(double p) {
		if (p >= 0.0D && p <= 1.0D) {
			return this.mean + this.sigma * Math.sqrt(2.0D) * erfInv(2.0D * p - 1.0D);
		} else {
			String error = "" + Double.valueOf(p) + ", " + Integer.valueOf(0) + ", " + Integer.valueOf(1);
			throw new IllegalArgumentException(error);
		}
	}


	public static double erfInv(double x) {
		double w = -Math.log((1.0D - x) * (1.0D + x));
		double p;
		if (w < 6.25D) {
			w -= 3.125D;
			p = -3.64441206401782E-21D;
			p = -1.6850591381820166E-19D + p * w;
			p = 1.28584807152564E-18D + p * w;
			p = 1.1157877678025181E-17D + p * w;
			p = -1.333171662854621E-16D + p * w;
			p = 2.0972767875968562E-17D + p * w;
			p = 6.637638134358324E-15D + p * w;
			p = -4.054566272975207E-14D + p * w;
			p = -8.151934197605472E-14D + p * w;
			p = 2.6335093153082323E-12D + p * w;
			p = -1.2975133253453532E-11D + p * w;
			p = -5.415412054294628E-11D + p * w;
			p = 1.0512122733215323E-9D + p * w;
			p = -4.112633980346984E-9D + p * w;
			p = -2.9070369957882005E-8D + p * w;
			p = 4.2347877827932404E-7D + p * w;
			p = -1.3654692000834679E-6D + p * w;
			p = -1.3882523362786469E-5D + p * w;
			p = 1.8673420803405714E-4D + p * w;
			p = -7.40702534166267E-4D + p * w;
			p = -0.006033670871430149D + p * w;
			p = 0.24015818242558962D + p * w;
			p = 1.6536545626831027D + p * w;
		} else if (w < 16.0D) {
			w = Math.sqrt(w) - 3.25D;
			p = 2.2137376921775787E-9D;
			p = 9.075656193888539E-8D + p * w;
			p = -2.7517406297064545E-7D + p * w;
			p = 1.8239629214389228E-8D + p * w;
			p = 1.5027403968909828E-6D + p * w;
			p = -4.013867526981546E-6D + p * w;
			p = 2.9234449089955446E-6D + p * w;
			p = 1.2475304481671779E-5D + p * w;
			p = -4.7318229009055734E-5D + p * w;
			p = 6.828485145957318E-5D + p * w;
			p = 2.4031110387097894E-5D + p * w;
			p = -3.550375203628475E-4D + p * w;
			p = 9.532893797373805E-4D + p * w;
			p = -0.0016882755560235047D + p * w;
			p = 0.002491442096107851D + p * w;
			p = -0.003751208507569241D + p * w;
			p = 0.005370914553590064D + p * w;
			p = 1.0052589676941592D + p * w;
			p = 3.0838856104922208D + p * w;
		} else if (!Double.isInfinite(w)) {
			w = Math.sqrt(w) - 5.0D;
			p = -2.7109920616438573E-11D;
			p = -2.555641816996525E-10D + p * w;
			p = 1.5076572693500548E-9D + p * w;
			p = -3.789465440126737E-9D + p * w;
			p = 7.61570120807834E-9D + p * w;
			p = -1.496002662714924E-8D + p * w;
			p = 2.914795345090108E-8D + p * w;
			p = -6.771199775845234E-8D + p * w;
			p = 2.2900482228026655E-7D + p * w;
			p = -9.9298272942317E-7D + p * w;
			p = 4.526062597223154E-6D + p * w;
			p = -1.968177810553167E-5D + p * w;
			p = 7.599527703001776E-5D + p * w;
			p = -2.1503011930044477E-4D + p * w;
			p = -1.3871931833623122E-4D + p * w;
			p = 1.0103004648645344D + p * w;
			p = 4.849906401408584D + p * w;
		} else {
			p = 1.0D / 0.0;
		}

		return p * x;
	}
}
