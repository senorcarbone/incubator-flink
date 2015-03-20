package org.apache.flink.streaming.examples.sampling;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.math3.*;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

/**
 * Created by marthavk on 2015-03-18.
 */
public class Evaluator implements Serializable {

	public static double evaluate(Distribution greal, Distribution gsampled) {

		//Bhattacharyya distance
		double m1 = greal.getMean();
		double m2 = gsampled.getMean();
		double s1 = greal.getStandardDeviation();
		double s2 = gsampled.getStandardDeviation();

		double factor1 = Math.pow(s1, 2) / Math.pow(s2, 2) + Math.pow(s2, 2) / Math.pow(s1, 2) + 2;
		double factor2 = Math.sqrt(m1 - m2) / (Math.sqrt(s1) + Math.sqrt(s2));
		double distance = (0.25) * Math.log((0.25) * factor1) + (0.25) * factor2;
		if (Double.isNaN(distance)) {
			System.out.println("m1=" + m1 + " m2=" + m2 + " m1-m2=" + (m1 - m2) + " Math.sqrt(m1-m2)=" + Math.sqrt(m1 - m2));
		}
		return distance;

	}

}
