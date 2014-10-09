package org.apache.flink.streaming.util.nextGenDeltaFunction;

import org.apache.flink.streaming.api.invokable.operator.NextGenConversionAwareDeltaFunction;
import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class CosineDistance<DATA> extends NextGenConversionAwareDeltaFunction<DATA, double[]>{

	/**
	 * auto-generated id
	 */
	private static final long serialVersionUID = -1217813582965151599L;
	
	public CosineDistance() {
		super(null);
	}
	
	public CosineDistance(NextGenExtractor<DATA, double[]> converter) {
		super(converter);
	}
	
	@Override
	public double getNestedDelta(double[] oldDataPoint, double[] newDataPoint) {
		double sum1=0;
		double sum2=0;
		for (int i=0;i<oldDataPoint.length;i++){
			sum1+=oldDataPoint[i]*oldDataPoint[i];
			sum2+=newDataPoint[i]*newDataPoint[i];
		}
		sum1=Math.sqrt(sum1);
		sum2=Math.sqrt(sum2);
		
		return dotProduct(oldDataPoint,newDataPoint)/(sum1*sum2);
	}
	
	private double dotProduct(double[] a,double[] b){
		double result=0;
		for (int i=0;i<a.length;i++){
			result+=a[i]*b[i];
		}
		return result;
	}

}
