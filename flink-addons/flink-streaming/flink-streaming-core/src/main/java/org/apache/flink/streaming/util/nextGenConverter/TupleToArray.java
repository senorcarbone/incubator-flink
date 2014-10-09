package org.apache.flink.streaming.util.nextGenConverter;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class TupleToArray implements NextGenExtractor<Tuple, double[]> {

	/**
	 * Auto generated version id
	 */
	private static final long serialVersionUID = -6076121226427616818L;
	int[] order=null;
	
	public TupleToArray() {
		// noting to do
	}
	
	public TupleToArray(int... indexes) {
		this.order=indexes;
	}
		
	@Override
	public double[] convert(Tuple in) {
		double[] output;
		
		if (order==null){
			//copy the hole tuple
			output=new double[in.getArity()];
			for (int i=0;i<in.getArity();i++){
				output[i]=in.getField(i);
			}
		} else {
			//copy user specified order
			output=new double[order.length];
			for (int i=0;i<order.length;i++){
				output[i]=in.getField(order[i]);
			}
		}
		
		return output;
	}

}
