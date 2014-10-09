package org.apache.flink.streaming.util.nextGenConverter;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.operator.NextGenTypeExtractor;

public class TupleToArray implements NextGenTypeExtractor<Tuple, int[]> {

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
	public int[] convert(Tuple in) {
		int[] output;
		
		if (order==null){
			//copy the hole tuple
			output=new int[in.getArity()];
			for (int i=0;i<in.getArity();i++){
				output[i]=in.getField(i);
			}
		} else {
			//copy user specified order
			output=new int[order.length];
			for (int i=0;i<order.length;i++){
				output[i]=in.getField(order[i]);
			}
		}
		
		return output;
	}

}
