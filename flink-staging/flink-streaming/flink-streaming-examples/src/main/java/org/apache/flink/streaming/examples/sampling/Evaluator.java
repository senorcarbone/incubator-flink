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
        double s1 = greal.getSigma();
        double s2 = gsampled.getSigma();

        double factor1 = Math.sqrt(s1)/Math.sqrt(s2) + Math.sqrt(s2)/Math.sqrt(s1) + 2;
        double factor2 = Math.sqrt(m1-m2)/(Math.sqrt(s1)+Math.sqrt(s2));
        return (1/4)*Math.log((1/4)*factor1) + (1/4)*factor2;

    }

}
