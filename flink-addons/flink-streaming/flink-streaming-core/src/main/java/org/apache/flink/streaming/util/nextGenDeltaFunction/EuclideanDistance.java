package org.apache.flink.streaming.util.nextGenDeltaFunction;

import org.apache.flink.streaming.api.invokable.operator.NextGenConversionAwareDeltaFunction;
import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class EuclideanDistance<DATA> extends NextGenConversionAwareDeltaFunction<DATA, double[]>{

	public EuclideanDistance(){
		super(null);
	}
	
	public EuclideanDistance(NextGenExtractor<DATA, double[]> converter) {
		super(converter);
	}

	/**
	 * auto-generated version id
	 */
	private static final long serialVersionUID = 3119432599634512359L;

	@Override
	public double getNestedDelta(double[] oldDataPoint, double[] newDataPoint) {
		double result=0;
		for (int i=0;i<oldDataPoint.length;i++){
			result+=(oldDataPoint[i]-newDataPoint[i])*(oldDataPoint[i]-newDataPoint[i]);
		}
		return Math.sqrt(result);
	}

}
